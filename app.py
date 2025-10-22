import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

import pandas as pd
import paramiko
import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, filedialog
from tkinter import ttk

# ----------------- CONFIG -----------------
APP_FOLDER = Path(__file__).parent
BAZA_XLSX = APP_FOLDER / "baza.xlsx"     # kolumny: nr lokalizacji, miejscowość, System Monitoringu
DB_PATH = APP_FOLDER / "downloads.db"
DEFAULT_LOCAL_ROOT = Path(r"D:\DO ZROBIENIA\Nowe Zgrywanie")
SFTP_RETRY_WINDOW_SECONDS = 14 * 60  # 14 minut
SSH_CONNECT_TIMEOUT = 10
# ------------------------------------------

# ----------------- UTIL: IP conversion -----------------
def shop_to_ip(shop: str) -> str:
    s = shop.strip()
    if not re.fullmatch(r"\d{5}", s):
        raise ValueError("Shop must be exactly 5 digits")
    p = int(s[0:2])  # 10..17
    X = s[2]
    YZ = s[3:5]
    if p in (10, 11):
        base = '8' + X
    elif p in (12, 13):
        base = '5' + X
    elif p in (14, 15):
        base = '19' + X
    elif p in (16, 17):
        base = '15' + X
    else:
        raise ValueError('Shop number outside supported ranges (10001-17999)')
    third = ('1' + YZ) if (p % 2 == 1) else YZ
    return f"10.{base}.{third}.99"

# ----------------- DB: queue/status -----------------
class DB:
    def __init__(self, path: Path):
        self.path = path
        self._init_db()

    def _init_db(self):
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        cur = self.conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                shop TEXT,
                city TEXT,
                camera TEXT,
                remote_path TEXT,
                filename TEXT,
                local_path TEXT,
                status TEXT,
                retries INTEGER DEFAULT 0,
                last_error TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        self.conn.commit()

    def add_task(self, shop, city, camera, remote_path, filename, local_path):
        cur = self.conn.cursor()
        now = datetime.utcnow().isoformat()
        cur.execute('''INSERT INTO tasks (shop,city,camera,remote_path,filename,local_path,status,created_at,updated_at)
                       VALUES (?,?,?,?,?,?,?, ?,?)''', (shop,city,camera,remote_path,filename,local_path,'pending', now, now))
        self.conn.commit()
        return cur.lastrowid

    def update_task(self, task_id, **kwargs):
        cur = self.conn.cursor()
        fields = []
        vals = []
        for k, v in kwargs.items():
            fields.append(f"{k} = ?")
            vals.append(v)
        vals.append(task_id)
        sql = f"UPDATE tasks SET {', '.join(fields)}, updated_at = ? WHERE id = ?"
        cur.execute(sql, (*vals[:-1], datetime.utcnow().isoformat(), vals[-1]))
        self.conn.commit()

    def list_tasks(self, statuses: Optional[List[str]] = None):
        cur = self.conn.cursor()
        if statuses:
            q = f"SELECT * FROM tasks WHERE status IN ({','.join('?' for _ in statuses)}) ORDER BY id"
            cur.execute(q, statuses)
        else:
            cur.execute('SELECT * FROM tasks ORDER BY id')
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

# ----------------- SFTP downloader -----------------
class SFTPDownloader:
    def __init__(self, host, port, username, password, db: DB, gui_callback=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db = db
        self.gui_callback = gui_callback
        self._stop = False

    def stop(self):
        self._stop = True

    def _connect(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.host, port=self.port, username=self.username, password=self.password, timeout=SSH_CONNECT_TIMEOUT)
        sftp = client.open_sftp()
        return client, sftp

    @staticmethod
    def _parse_event_datetime_from_name(name: str) -> Optional[datetime]:
        m = re.search(r"(\d{14})", name)
        if not m:
            return None
        try:
            return datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
        except Exception:
            return None

    def discover_and_queue(self, *, remote_base_dir: str, cam_pattern: str, autoselect_cams: bool,
                            dt_from: datetime, dt_to: datetime, time_source: str, offset_min: int,
                            local_root: Path, shop: str, city: str):
        # compute UTC window from local user input (for mtime path)
        local_offset = datetime.now().astimezone().utcoffset() or timedelta(0)
        user_from_utc = dt_from - local_offset
        user_to_utc   = dt_to   - local_offset

        client, sftp = self._connect()
        try:
            entries = sftp.listdir(remote_base_dir)
            if autoselect_cams:
                cams = [n for n in entries if re.fullmatch(r"cam\d{2}", n, flags=re.IGNORECASE)]
            else:
                cams = [n for n in entries if re.fullmatch(cam_pattern, n, flags=re.IGNORECASE)]

            for cam in cams:
                cam_remote = f"{remote_base_dir}/{cam}"
                try:
                    date_folders = sftp.listdir(cam_remote)
                except Exception:
                    continue
                for date_folder in date_folders:
                    sub_remote = f"{cam_remote}/{date_folder}"
                    try:
                        attrs = sftp.listdir_attr(sub_remote)
                    except Exception:
                        continue
                    for a in attrs:
                        fname = a.filename
                        if not fname.lower().endswith('.avi'):
                            continue
                        # Źródło czasu: mtime (z offsetem) albo nazwa pliku
                        if time_source == 'mtime':
                            try:
                                file_dt_utc = datetime.utcfromtimestamp(a.st_mtime) + timedelta(minutes=offset_min)
                            except Exception:
                                continue
                            # strict UTC comparison vs. user range converted to UTC
                            if not (user_from_utc <= file_dt_utc <= user_to_utc):
                                continue
                        else:
                            dt_candidate = self._parse_event_datetime_from_name(fname)
                            if not dt_candidate:
                                continue
                        if not (dt_from <= dt_candidate <= dt_to):
                            continue
                        rel_local = Path(cam) / fname
                        local_path = local_root / rel_local
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        self.db.add_task(shop=shop, city=city, camera=cam, remote_path=sub_remote,
                                         filename=fname, local_path=str(local_path))
            # worker
            worker = threading.Thread(target=self._process_pending, args=(client, sftp), daemon=True)
            worker.start()
        except Exception:
            try:
                sftp.close(); client.close()
            except Exception:
                pass
            raise

    def _process_pending(self, client: paramiko.SSHClient, sftp: paramiko.SFTPClient):
        while not self._stop:
            tasks = self.db.list_tasks(['pending', 'error'])
            if not tasks:
                time.sleep(0.8)
                continue
            for t in tasks:
                if self._stop:
                    break
                task_id = t['id']
                if t['status'] == 'done':
                    continue
                self.db.update_task(task_id, status='downloading')
                try:
                    started = time.time()
                    success = self._download_with_retries(sftp, t, started)
                    if success:
                        self.db.update_task(task_id, status='done', retries=t['retries'])
                    else:
                        self.db.update_task(task_id, status='error', last_error='timeout or failed', retries=t['retries'] + 1)
                except Exception as e:
                    self.db.update_task(task_id, status='error', last_error=str(e), retries=t['retries'] + 1)
                if self.gui_callback:
                    try:
                        self.gui_callback()
                    except Exception:
                        pass
            time.sleep(0.3)
        try:
            sftp.close(); client.close()
        except Exception:
            pass

    def _download_with_retries(self, sftp: paramiko.SFTPClient, task: dict, started_time: float) -> bool:
        remote_full = f"{task['remote_path']}/{task['filename']}"
        local_path = Path(task['local_path'])
        temp_local = local_path.with_suffix('.part')
        attempt = 0
        while time.time() - started_time < SFTP_RETRY_WINDOW_SECONDS:
            attempt += 1
            try:
                with sftp.open(remote_full, 'rb') as rf, open(temp_local, 'wb') as lf:
                    while True:
                        chunk = rf.read(32768)
                        if not chunk:
                            break
                        lf.write(chunk)
                        if self._stop:
                            raise RuntimeError('Stopped by user')
                temp_local.replace(local_path)
                return True
            except Exception as e:
                self.db.update_task(task['id'], last_error=f"attempt {attempt}: {e}", retries=t['retries'] + attempt)
                time.sleep(2)
        return False

# ----------------- GUI -----------------
class App:
    def __init__(self, root: tb.Window, db: DB):
        self.root = root
        self.db = db
        root.title('Dino Camera Downloader')
        root.geometry('1120x700')

        # --- Layout: two columns ---
        container = tb.Frame(root, padding=12)
        container.pack(fill='both', expand=True)
        left = tb.Frame(container)
        right = tb.Frame(container)
        left.pack(side='left', fill='y', padx=(0,12))
        right.pack(side='left', fill='both', expand=True)

        # --- Left: sections ---
        # Shop / localization
        sec_shop = tb.Labelframe(left, text='Sklep / Lokalizacja', padding=10)
        sec_shop.pack(fill='x', pady=6)
        self.shop_var = tb.StringVar()
        self.city_var = tb.StringVar()
        self.sys_var  = tb.StringVar()
        self.ip_var   = tb.StringVar(value='-')
        row=0
        tb.Label(sec_shop, text='Numer sklepu (5 cyfr)').grid(row=row, column=0, sticky='w');
        tb.Entry(sec_shop, textvariable=self.shop_var, width=14).grid(row=row, column=1, sticky='w', padx=6);
        tb.Button(sec_shop, text='Wczytaj z baza.xlsx', command=self.load_from_baza).grid(row=row, column=2, padx=4)
        row+=1
        tb.Label(sec_shop, text='Miejscowość').grid(row=row, column=0, sticky='w');
        tb.Entry(sec_shop, textvariable=self.city_var, width=28).grid(row=row, column=1, sticky='w', padx=6, columnspan=2)
        row+=1
        tb.Label(sec_shop, text='System Monitoringu').grid(row=row, column=0, sticky='w');
        tb.Entry(sec_shop, textvariable=self.sys_var, state='readonly', width=28).grid(row=row, column=1, sticky='w', padx=6, columnspan=2)
        row+=1
        tb.Label(sec_shop, text='IP docelowe').grid(row=row, column=0, sticky='w');
        tb.Label(sec_shop, textvariable=self.ip_var).grid(row=row, column=1, sticky='w', padx=6)

        # Range + time source
        sec_rng = tb.Labelframe(left, text='Zakres dat/godzin', padding=10)
        sec_rng.pack(fill='x', pady=6)
        self.dt_from = tb.StringVar(value=(datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d 00:00'))
        self.dt_to   = tb.StringVar(value=datetime.now().strftime('%Y-%m-%d %H:%M'))
        tb.Label(sec_rng, text='FROM (YYYY-MM-DD HH:MM)').grid(row=0, column=0, sticky='w')
        tb.Entry(sec_rng, textvariable=self.dt_from, width=20).grid(row=0, column=1, sticky='w', padx=6)
        tb.Label(sec_rng, text='TO (YYYY-MM-DD HH:MM)').grid(row=0, column=2, sticky='w')
        tb.Entry(sec_rng, textvariable=self.dt_to, width=20).grid(row=0, column=3, sticky='w', padx=6)
        # time source + offset
        self.time_source = tb.StringVar(value='mtime')  # 'mtime' or 'fname'
        self.offset_min  = tb.IntVar(value=0)
        tb.Radiobutton(sec_rng, text='Filtr po mtime (zalecane)', variable=self.time_source, value='mtime').grid(row=1, column=0, columnspan=2, sticky='w', pady=(6,0))
        tb.Radiobutton(sec_rng, text='Filtr po dacie w nazwie', variable=self.time_source, value='fname').grid(row=1, column=2, columnspan=2, sticky='w', pady=(6,0))
        tb.Label(sec_rng, text='Offset mtime (min)').grid(row=2, column=0, sticky='w', pady=(6,0))
        tb.Entry(sec_rng, textvariable=self.offset_min, width=8).grid(row=2, column=1, sticky='w', padx=6, pady=(6,0))

        # Connection
        sec_conn = tb.Labelframe(left, text='Połączenie SSH/SFTP', padding=10)
        sec_conn.pack(fill='x', pady=6)
        self.ssh_host = tb.StringVar(value='10.124.56.128')
        self.ssh_port = tb.IntVar(value=22)
        self.ssh_user = tb.StringVar(value='dino')
        self.ssh_pass = tb.StringVar(value='dino')
        grid=[('Host',self.ssh_host),('Port',self.ssh_port),('User',self.ssh_user),('Hasło',self.ssh_pass)]
        for i,(lbl,var) in enumerate(grid):
            tb.Label(sec_conn, text=lbl).grid(row=i, column=0, sticky='w')
            tb.Entry(sec_conn, textvariable=var, width=28, show='*' if lbl=='Hasło' else None).grid(row=i, column=1, sticky='w', padx=6)

        # Paths
        sec_paths = tb.Labelframe(left, text='Ścieżki i kamery', padding=10)
        sec_paths.pack(fill='x', pady=6)
        self.remote_base = tb.StringVar(value='D:/Kamery')
        self.cam_pattern = tb.StringVar(value=r'cam\d{2}')
        self.autocams    = tb.BooleanVar(value=True)
        self.local_root  = tb.StringVar(value=str(DEFAULT_LOCAL_ROOT))
        tb.Label(sec_paths, text='Remote base').grid(row=0, column=0, sticky='w')
        tb.Entry(sec_paths, textvariable=self.remote_base, width=28).grid(row=0, column=1, sticky='w', padx=6)
        tb.Checkbutton(sec_paths, text='Autowybór wszystkich camXX', variable=self.autocams, bootstyle='round-toggle').grid(row=1, column=0, columnspan=2, sticky='w', pady=(6,0))
        tb.Label(sec_paths, text='Wzorzec kamer (regex)').grid(row=2, column=0, sticky='w')
        tb.Entry(sec_paths, textvariable=self.cam_pattern, width=18).grid(row=2, column=1, sticky='w', padx=6)
        tb.Label(sec_paths, text='Lokalny root').grid(row=3, column=0, sticky='w')
        tb.Entry(sec_paths, textvariable=self.local_root, width=28).grid(row=3, column=1, sticky='w', padx=6)
        tb.Button(sec_paths, text='Wybierz...', command=self.choose_local_root).grid(row=3, column=2, padx=6)

        # Actions
        sec_actions = tb.Frame(left)
        sec_actions.pack(fill='x', pady=8)
        tb.Button(sec_actions, text='Zbuduj kolejkę i start', bootstyle='success', command=self.populate_and_start).pack(side='left', padx=4)
        tb.Button(sec_actions, text='Stop', bootstyle='danger', command=self.stop_downloader).pack(side='left', padx=4)

        # --- Right: queue table ---
        tb.Label(right, text='Kolejka zadań', font=('TkDefaultFont', 10, 'bold')).pack(anchor='w')
        table_frame = tb.Frame(right)
        table_frame.pack(fill='both', expand=True, pady=(6,0))
        cols=('id','shop','city','camera','filename','status','retries')
        self.tree = tb.Treeview(table_frame, columns=cols, show='headings')
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120 if c!='filename' else 360, anchor='w')
        ysb = ttk.Scrollbar(table_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscroll=ysb.set)
        self.tree.pack(side='left', fill='both', expand=True)
        ysb.pack(side='right', fill='y')

        btns = tb.Frame(right)
        btns.pack(fill='x', pady=6)
        tb.Button(btns, text='Odśwież', command=self.refresh_tasks).pack(side='left', padx=4)
        tb.Button(btns, text='Ponów zaznaczone', command=self.retry_selected).pack(side='left', padx=4)
        tb.Button(btns, text='Otwórz lokalny root', command=self.open_local_root).pack(side='left', padx=4)

        self.statusbar = tb.Label(root, text='Ready', anchor='w')
        self.statusbar.pack(fill='x', side='bottom')

        self.downloader: Optional[SFTPDownloader] = None
        self.shop_var.trace_add('write', lambda *_: self._update_ip())
        self.refresh_tasks()
        self._update_ip()

    # --- helpers ---
    def _update_ip(self):
        s = (self.shop_var.get() or '').strip()
        if re.fullmatch(r"\d{5}", s):
            try:
                self.ip_var.set(shop_to_ip(s))
            except Exception as e:
                self.ip_var.set(str(e))
        else:
            self.ip_var.set('-')

    def load_from_baza(self):
        shop = self.shop_var.get().strip()
        if not re.fullmatch(r"\d{5}", shop):
            messagebox.showerror('Błąd', 'Numer sklepu musi mieć dokładnie 5 cyfr')
            return
        if not BAZA_XLSX.exists():
            messagebox.showerror('Błąd', f'Brak pliku: {BAZA_XLSX}')
            return
        df = pd.read_excel(BAZA_XLSX)
        cols = {c.lower().strip(): c for c in df.columns}
        nr_col = cols.get('nr lokalizacji') or cols.get('nr_lokalizacji') or list(df.columns)[0]
        town_col = cols.get('miejscowość') or cols.get('miejscowosc') or list(df.columns)[1]
        sys_col = cols.get('system monitoringu') or cols.get('system_monitoringu') or list(df.columns)[2]
        row = df[df[nr_col].astype(str).str.zfill(5) == shop]
        if row.empty:
            messagebox.showwarning('Brak', 'Sklep nie znaleziony w baza.xlsx')
            return
        r = row.iloc[0]
        city = str(r[town_col])
        city_clean = re.sub(r"^D\.\s*", '', city)
        self.city_var.set(city_clean)
        self.sys_var.set(str(r[sys_col]))
        if str(r[sys_col]).strip().upper() == 'HIKVISION':
            messagebox.showerror('Niewspierane', 'HIKVISION nie jest obsługiwany — zlecenie zablokowane.')

    def choose_local_root(self):
        p = filedialog.askdirectory()
        if p:
            self.local_root.set(p)

    def populate_and_start(self):
        shop = self.shop_var.get().strip()
        if not re.fullmatch(r"\d{5}", shop):
            messagebox.showerror('Błąd', 'Numer sklepu musi mieć dokładnie 5 cyfr')
            return
        sysmon = (self.sys_var.get() or '').strip().upper()
        if sysmon == 'HIKVISION':
            messagebox.showerror('Niewspierane', 'HIKVISION nie jest obsługiwany')
            return
        city = (self.city_var.get() or f'shop_{shop}').strip()

        try:
            dtf = datetime.strptime(self.dt_from.get().strip(), '%Y-%m-%d %H:%M')
            dtt = datetime.strptime(self.dt_to.get().strip(), '%Y-%m-%d %H:%M')
        except Exception:
            messagebox.showerror('Błąd', 'Zły format daty. Użyj YYYY-MM-DD HH:MM')
            return
        if dtt < dtf:
            messagebox.showerror('Błąd', 'TO musi być >= FROM')
            return

        day_tag = dtf.strftime('%Y%m%d')
        safe_city = re.sub(r"[^0-9A-Za-ząćęłńóśźżĄĆĘŁŃÓŚŹŻ _-]", '', city)
        local_root = Path(self.local_root.get()) / f"{safe_city}_{day_tag}"
        local_root.mkdir(parents=True, exist_ok=True)

        host = self.ssh_host.get().strip()
        port = int(self.ssh_port.get())
        user = self.ssh_user.get().strip()
        pwd  = self.ssh_pass.get()
        remote_base = self.remote_base.get().strip().replace('\\', '/')
        cam_pattern = self.cam_pattern.get().strip()
        autocams    = bool(self.autocams.get())
        time_source = self.time_source.get()
        offset_min  = int(self.offset_min.get())

        self.downloader = SFTPDownloader(host, port, user, pwd, self.db, gui_callback=self.refresh_tasks)
        t = threading.Thread(target=self.downloader.discover_and_queue, kwargs=dict(
            remote_base_dir=remote_base,
            cam_pattern=cam_pattern,
            autoselect_cams=autocams,
            dt_from=dtf, dt_to=dtt,
            time_source=time_source,
            offset_min=offset_min,
            local_root=local_root,
            shop=shop, city=city
        ), daemon=True)
        t.start()
        self.statusbar.config(text='Kolejka budowana — start pobierania...')

    def stop_downloader(self):
        if self.downloader:
            self.downloader.stop()
            self.statusbar.config(text='Stop requested')

    def refresh_tasks(self):
        for r in self.tree.get_children():
            self.tree.delete(r)
        tasks = self.db.list_tasks()
        for t in tasks:
            self.tree.insert('', 'end', values=(t['id'], t['shop'], t['city'], t['camera'], t['filename'], t['status'], t['retries']))
        self.statusbar.config(text=f'{len(tasks)} zadań w kolejce')

    def retry_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        for iid in sel:
            vals = self.tree.item(iid, 'values')
            task_id = int(vals[0])
            self.db.update_task(task_id, status='pending', last_error=None, retries=0)
        self.refresh_tasks()

    def open_local_root(self):
        p = Path(self.local_root.get())
        if not p.exists():
            messagebox.showinfo('Info', 'Folder lokalny jeszcze nie istnieje')
            return
        os.startfile(str(p))

# ----------------- Main -----------------
if __name__ == '__main__':
    db = DB(DB_PATH)
    app = tb.Window(themename='flatly')
    App(app, db)
    app.mainloop()
