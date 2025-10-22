import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple

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
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY,
                shop TEXT,
                city TEXT,
                dt_from TEXT,
                dt_to TEXT,
                local_root TEXT,
                remote_base TEXT,
                time_source TEXT,
                status TEXT,
                total_files INTEGER DEFAULT 0,
                done_files INTEGER DEFAULT 0,
                error_files INTEGER DEFAULT 0,
                last_error TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
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
                updated_at TEXT,
                job_id INTEGER,
                remote_time TEXT
            )
        ''')
        cur.execute('PRAGMA table_info(tasks)')
        task_cols = {row[1] for row in cur.fetchall()}
        if 'job_id' not in task_cols:
            cur.execute('ALTER TABLE tasks ADD COLUMN job_id INTEGER')
        if 'remote_time' not in task_cols:
            cur.execute('ALTER TABLE tasks ADD COLUMN remote_time TEXT')
        cur.execute('PRAGMA table_info(jobs)')
        job_cols = {row[1] for row in cur.fetchall()}
        if 'total_files' not in job_cols:
            cur.execute('ALTER TABLE jobs ADD COLUMN total_files INTEGER DEFAULT 0')
        if 'done_files' not in job_cols:
            cur.execute('ALTER TABLE jobs ADD COLUMN done_files INTEGER DEFAULT 0')
        if 'error_files' not in job_cols:
            cur.execute('ALTER TABLE jobs ADD COLUMN error_files INTEGER DEFAULT 0')
        if 'last_error' not in job_cols:
            cur.execute('ALTER TABLE jobs ADD COLUMN last_error TEXT')
        self.conn.commit()

    def create_job(self, *, shop: str, city: str, dt_from: datetime, dt_to: datetime,
                   local_root: Path, remote_base: str, time_source: str) -> int:
        cur = self.conn.cursor()
        now = datetime.utcnow().isoformat()
        cur.execute('''
            INSERT INTO jobs (shop, city, dt_from, dt_to, local_root, remote_base, time_source, status, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        ''', (
            shop,
            city,
            dt_from.isoformat(timespec='minutes'),
            dt_to.isoformat(timespec='minutes'),
            str(local_root),
            remote_base,
            time_source,
            'discovering',
            now,
            now
        ))
        self.conn.commit()
        return cur.lastrowid

    def update_job(self, job_id: int, **kwargs):
        if not kwargs:
            return
        cur = self.conn.cursor()
        fields = []
        vals = []
        for k, v in kwargs.items():
            fields.append(f"{k} = ?")
            vals.append(v)
        vals.append(datetime.utcnow().isoformat())
        vals.append(job_id)
        sql = f"UPDATE jobs SET {', '.join(fields)}, updated_at = ? WHERE id = ?"
        cur.execute(sql, tuple(vals))
        self.conn.commit()

    def refresh_job_status(self, job_id: int):
        cur = self.conn.cursor()
        cur.execute('''
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors,
                SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END) as downloading,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
            FROM tasks
            WHERE job_id = ?
        ''', (job_id,))
        row = cur.fetchone()
        if row is None:
            return
        total, done, errors, downloading, pending = row
        done = done or 0
        errors = errors or 0
        downloading = downloading or 0
        pending = pending or 0
        if total == 0:
            status = 'no_files'
        elif done == total:
            status = 'done'
        elif downloading > 0:
            status = 'downloading'
        elif pending > 0:
            status = 'pending'
        elif errors > 0:
            status = 'error'
        else:
            status = 'mixed'
        self.update_job(job_id, status=status, total_files=total, done_files=done, error_files=errors)

    def list_jobs(self):
        cur = self.conn.cursor()
        cur.execute('''
            SELECT id, shop, city, dt_from, dt_to, local_root, remote_base, time_source,
                   status, total_files, done_files, error_files, last_error, created_at, updated_at
            FROM jobs
            ORDER BY datetime(created_at) DESC, id DESC
        ''')
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_job(self, job_id: int):
        cur = self.conn.cursor()
        cur.execute('''
            SELECT id, shop, city, dt_from, dt_to, local_root, remote_base, time_source,
                   status, total_files, done_files, error_files, last_error, created_at, updated_at
            FROM jobs WHERE id = ?
        ''', (job_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))

    def add_task(self, shop, city, camera, remote_path, filename, local_path, *, job_id: int, remote_time: Optional[str]):
        cur = self.conn.cursor()
        now = datetime.utcnow().isoformat()
        cur.execute('''
            INSERT INTO tasks (shop,city,camera,remote_path,filename,local_path,status,created_at,updated_at,job_id,remote_time)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ''', (shop,city,camera,remote_path,filename,local_path,'pending', now, now, job_id, remote_time))
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

    def get_task(self, task_id: int):
        cur = self.conn.cursor()
        cur.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))

    def list_tasks(self, statuses: Optional[List[str]] = None, job_id: Optional[int] = None):
        cur = self.conn.cursor()
        filters = []
        vals: List = []
        if statuses:
            filters.append(f"status IN ({','.join('?' for _ in statuses)})")
            vals.extend(statuses)
        if job_id is not None:
            filters.append('job_id = ?')
            vals.append(job_id)
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ''
        cur.execute(f'SELECT * FROM tasks {where_clause} ORDER BY id', tuple(vals))
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
                            local_root: Path, shop: str, city: str, job_id: int):
        # compute UTC window from local user input (for mtime path)
        local_offset = datetime.now().astimezone().utcoffset() or timedelta(0)
        user_from_utc = dt_from - local_offset
        user_to_utc   = dt_to   - local_offset

        client, sftp = self._connect()
        self.db.update_job(job_id, status='discovering', last_error=None)
        added = 0
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
                        dt_candidate: Optional[datetime]
                        # Źródło czasu: mtime (z offsetem) albo nazwa pliku
                        if time_source == 'mtime':
                            try:
                                file_dt_utc = datetime.utcfromtimestamp(a.st_mtime) + timedelta(minutes=offset_min)
                            except Exception:
                                continue
                            if not (user_from_utc <= file_dt_utc <= user_to_utc):
                                continue
                            dt_candidate = (file_dt_utc + local_offset)
                        else:
                            dt_candidate = self._parse_event_datetime_from_name(fname)
                            if not dt_candidate:
                                continue
                        if not (dt_from <= dt_candidate <= dt_to):
                            continue
                        rel_local = Path(cam) / fname
                        local_path = local_root / rel_local
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        remote_time = dt_candidate.strftime('%Y-%m-%d %H:%M:%S') if dt_candidate else None
                        self.db.add_task(shop=shop, city=city, camera=cam, remote_path=sub_remote,
                                         filename=fname, local_path=str(local_path), job_id=job_id, remote_time=remote_time)
                        added += 1
            if added == 0:
                self.db.update_job(job_id, status='no_files')
            else:
                self.db.refresh_job_status(job_id)
            # worker
            worker = threading.Thread(target=self._process_pending, args=(client, sftp), daemon=True)
            worker.start()
        except Exception as exc:
            try:
                sftp.close(); client.close()
            except Exception:
                pass
            self.db.update_job(job_id, status='error', last_error=str(exc))
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
                if t.get('job_id'):
                    self.db.refresh_job_status(t['job_id'])
                try:
                    started = time.time()
                    success, retries = self._download_with_retries(sftp, t, started)
                    if success:
                        self.db.update_task(task_id, status='done', retries=retries)
                        t['retries'] = retries
                    else:
                        self.db.update_task(task_id, status='error', last_error='timeout or failed', retries=retries)
                        t['retries'] = retries
                except Exception as e:
                    new_retries = (t['retries'] or 0) + 1
                    self.db.update_task(task_id, status='error', last_error=str(e), retries=new_retries)
                    t['retries'] = new_retries
                if self.gui_callback:
                    try:
                        self.gui_callback()
                    except Exception:
                        pass
                if t.get('job_id'):
                    self.db.refresh_job_status(t['job_id'])
            time.sleep(0.3)
        try:
            sftp.close(); client.close()
        except Exception:
            pass

    def _download_with_retries(self, sftp: paramiko.SFTPClient, task: dict, started_time: float) -> Tuple[bool, int]:
        remote_full = f"{task['remote_path']}/{task['filename']}"
        local_path = Path(task['local_path'])
        temp_local = local_path.with_suffix('.part')
        attempt = 0
        retries = task.get('retries') or 0
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
                return True, retries
            except Exception as e:
                retries += 1
                self.db.update_task(task['id'], last_error=f"attempt {attempt}: {e}", retries=retries)
                time.sleep(2)
        return False, retries

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
        default_to = datetime.now().replace(second=0, microsecond=0)
        default_from = (default_to - timedelta(days=1)).replace(minute=0)
        self.date_from_var = tb.StringVar(value=default_from.strftime('%Y-%m-%d'))
        self.hour_from_var = tb.StringVar(value=default_from.strftime('%H'))
        self.min_from_var = tb.StringVar(value=default_from.strftime('%M'))
        self.date_to_var = tb.StringVar(value=default_to.strftime('%Y-%m-%d'))
        self.hour_to_var = tb.StringVar(value=default_to.strftime('%H'))
        self.min_to_var = tb.StringVar(value=default_to.strftime('%M'))
        tb.Label(sec_rng, text='Data od').grid(row=0, column=0, sticky='w')
        self.date_from_entry = tb.DateEntry(sec_rng, textvariable=self.date_from_var, dateformat='%Y-%m-%d', width=12)
        self.date_from_entry.grid(row=0, column=1, sticky='w', padx=6)
        tb.Label(sec_rng, text='Godzina od').grid(row=0, column=2, sticky='w')
        self.hour_from_spin = ttk.Spinbox(sec_rng, from_=0, to=23, width=3, wrap=True, textvariable=self.hour_from_var)
        self.hour_from_spin.grid(row=0, column=3, sticky='w', padx=2)
        self.min_from_spin = ttk.Spinbox(sec_rng, from_=0, to=59, width=3, wrap=True, textvariable=self.min_from_var)
        self.min_from_spin.grid(row=0, column=4, sticky='w', padx=(0,6))
        tb.Label(sec_rng, text='Data do').grid(row=1, column=0, sticky='w', pady=(6,0))
        self.date_to_entry = tb.DateEntry(sec_rng, textvariable=self.date_to_var, dateformat='%Y-%m-%d', width=12)
        self.date_to_entry.grid(row=1, column=1, sticky='w', padx=6, pady=(6,0))
        tb.Label(sec_rng, text='Godzina do').grid(row=1, column=2, sticky='w', pady=(6,0))
        self.hour_to_spin = ttk.Spinbox(sec_rng, from_=0, to=23, width=3, wrap=True, textvariable=self.hour_to_var)
        self.hour_to_spin.grid(row=1, column=3, sticky='w', padx=2, pady=(6,0))
        self.min_to_spin = ttk.Spinbox(sec_rng, from_=0, to=59, width=3, wrap=True, textvariable=self.min_to_var)
        self.min_to_spin.grid(row=1, column=4, sticky='w', padx=(0,6), pady=(6,0))
        # time source + offset
        self.time_source = tb.StringVar(value='mtime')  # 'mtime' or 'fname'
        self.offset_min  = tb.IntVar(value=0)
        tb.Radiobutton(sec_rng, text='Filtr po mtime (zalecane)', variable=self.time_source, value='mtime').grid(row=2, column=0, columnspan=2, sticky='w', pady=(10,0))
        tb.Radiobutton(sec_rng, text='Filtr po dacie w nazwie', variable=self.time_source, value='fname').grid(row=2, column=2, columnspan=2, sticky='w', pady=(10,0))
        tb.Label(sec_rng, text='Offset mtime (min)').grid(row=3, column=0, sticky='w', pady=(6,0))
        tb.Entry(sec_rng, textvariable=self.offset_min, width=8).grid(row=3, column=1, sticky='w', padx=6, pady=(6,0))

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

        # --- Right: jobs & tasks ---
        tb.Label(right, text='Zlecenia (sklep + zakres)', font=('TkDefaultFont', 10, 'bold')).pack(anchor='w')
        jobs_frame = tb.Frame(right)
        jobs_frame.pack(fill='x', pady=(6,0))
        job_cols = ('shop', 'city', 'range', 'progress', 'status')
        self.job_tree = tb.Treeview(jobs_frame, columns=job_cols, show='headings', height=8)
        for col, width in zip(job_cols, (90, 140, 260, 120, 120)):
            self.job_tree.heading(col, text=col.capitalize())
            self.job_tree.column(col, width=width, anchor='w')
        job_scroll = ttk.Scrollbar(jobs_frame, orient='vertical', command=self.job_tree.yview)
        self.job_tree.configure(yscroll=job_scroll.set)
        self.job_tree.pack(side='left', fill='x', expand=True)
        job_scroll.pack(side='right', fill='y')
        self.job_tree.bind('<<TreeviewSelect>>', self.on_job_select)

        job_btns = tb.Frame(right)
        job_btns.pack(fill='x', pady=6)
        tb.Button(job_btns, text='Odśwież', command=self.refresh_tasks).pack(side='left', padx=4)
        tb.Button(job_btns, text='Otwórz folder zlecenia', command=self.open_local_root).pack(side='left', padx=4)

        self.detail_title_var = tb.StringVar(value='Wybierz zlecenie, aby zobaczyć pliki')
        tb.Label(right, textvariable=self.detail_title_var, font=('TkDefaultFont', 10, 'bold')).pack(anchor='w', pady=(12,0))
        tasks_frame = tb.Frame(right)
        tasks_frame.pack(fill='both', expand=True, pady=(6,0))
        task_cols = ('camera', 'filename', 'remote_time', 'status', 'retries')
        self.task_tree = tb.Treeview(tasks_frame, columns=task_cols, show='headings')
        headings = {
            'camera': 'Kamera',
            'filename': 'Plik',
            'remote_time': 'Data (serwer)',
            'status': 'Status',
            'retries': 'Próby'
        }
        widths = {'camera': 100, 'filename': 320, 'remote_time': 150, 'status': 120, 'retries': 80}
        for col in task_cols:
            self.task_tree.heading(col, text=headings[col])
            self.task_tree.column(col, width=widths[col], anchor='w')
        task_scroll = ttk.Scrollbar(tasks_frame, orient='vertical', command=self.task_tree.yview)
        self.task_tree.configure(yscroll=task_scroll.set)
        self.task_tree.pack(side='left', fill='both', expand=True)
        task_scroll.pack(side='right', fill='y')

        task_btns = tb.Frame(right)
        task_btns.pack(fill='x', pady=6)
        tb.Button(task_btns, text='Ponów zaznaczone pliki', command=self.retry_selected_tasks).pack(side='left', padx=4)

        self.statusbar = tb.Label(root, text='Ready', anchor='w')
        self.statusbar.pack(fill='x', side='bottom')

        self.downloader: Optional[SFTPDownloader] = None
        self.selected_job_id: Optional[int] = None
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

    def _parse_datetime_range(self) -> Tuple[datetime, datetime]:
        date_from = self.date_from_var.get().strip()
        hour_from = self.hour_from_var.get().strip().zfill(2)
        minute_from = self.min_from_var.get().strip().zfill(2)
        date_to = self.date_to_var.get().strip()
        hour_to = self.hour_to_var.get().strip().zfill(2)
        minute_to = self.min_to_var.get().strip().zfill(2)
        dtf_str = f"{date_from} {hour_from}:{minute_from}"
        dtt_str = f"{date_to} {hour_to}:{minute_to}"
        dtf = datetime.strptime(dtf_str, '%Y-%m-%d %H:%M')
        dtt = datetime.strptime(dtt_str, '%Y-%m-%d %H:%M')
        return dtf, dtt

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
            dtf, dtt = self._parse_datetime_range()
        except Exception:
            messagebox.showerror('Błąd', 'Zły format daty. Użyj pól wyboru daty i godziny')
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

        self.downloader = SFTPDownloader(host, port, user, pwd, self.db, gui_callback=self._async_refresh)
        job_id = self.db.create_job(shop=shop, city=city, dt_from=dtf, dt_to=dtt,
                                    local_root=local_root, remote_base=remote_base, time_source=time_source)
        self.refresh_tasks()
        self._select_job(job_id)
        t = threading.Thread(target=self.downloader.discover_and_queue, kwargs=dict(
            remote_base_dir=remote_base,
            cam_pattern=cam_pattern,
            autoselect_cams=autocams,
            dt_from=dtf, dt_to=dtt,
            time_source=time_source,
            offset_min=offset_min,
            local_root=local_root,
            shop=shop, city=city, job_id=job_id
        ), daemon=True)
        t.start()
        self.statusbar.config(text='Kolejka budowana — start pobierania...')

    def stop_downloader(self):
        if self.downloader:
            self.downloader.stop()
            self.statusbar.config(text='Stop requested')

    def refresh_tasks(self):
        jobs = self.db.list_jobs()
        current = self._get_selected_job_id()
        if current is None:
            current = self.selected_job_id
        for iid in self.job_tree.get_children():
            self.job_tree.delete(iid)
        total_files = 0
        done_files = 0
        error_files = 0
        for job in jobs:
            job_id = job['id']
            total = job.get('total_files') or 0
            done = job.get('done_files') or 0
            errors = job.get('error_files') or 0
            total_files += total
            done_files += done
            error_files += errors
            range_text = f"{self._format_iso(job.get('dt_from'))} – {self._format_iso(job.get('dt_to'))}"
            status_text = job.get('status') or '-'
            if errors:
                status_text += f" (błędy: {errors})"
            progress = f"{done}/{total}" if total else '0/0'
            iid = f"job_{job_id}"
            self.job_tree.insert('', 'end', iid=iid, values=(job['shop'], job['city'], range_text, progress, status_text))
        if current and f"job_{current}" in self.job_tree.get_children(''):
            self.job_tree.selection_set(f"job_{current}")
            self.job_tree.see(f"job_{current}")
            selected = current
        else:
            selected = None
        self._populate_task_tree(selected)
        jobs_count = len(jobs)
        self.statusbar.config(text=f'Zlecenia: {jobs_count} | Pliki: {done_files}/{total_files} (błędy: {error_files})')

    def _async_refresh(self):
        self.root.after(0, self.refresh_tasks)

    def _get_selected_job_id(self) -> Optional[int]:
        sel = self.job_tree.selection()
        if not sel:
            return None
        iid = sel[0]
        if iid.startswith('job_'):
            try:
                return int(iid.split('_', 1)[1])
            except ValueError:
                return None
        return None

    def _select_job(self, job_id: int):
        iid = f"job_{job_id}"
        if iid in self.job_tree.get_children(''):
            self.job_tree.selection_set(iid)
            self.job_tree.see(iid)
            self._populate_task_tree(job_id)

    def _populate_task_tree(self, job_id: Optional[int]):
        for iid in self.task_tree.get_children():
            self.task_tree.delete(iid)
        if job_id is None:
            self.detail_title_var.set('Wybierz zlecenie, aby zobaczyć pliki')
            self.selected_job_id = None
            return
        tasks = self.db.list_tasks(job_id=job_id)
        for task in tasks:
            remote_time = self._format_remote(task.get('remote_time'))
            iid = f"task_{task['id']}"
            self.task_tree.insert('', 'end', iid=iid, values=(task['camera'], task['filename'], remote_time, task['status'], task['retries']))
        job = self.db.get_job(job_id)
        if job:
            range_text = f"{self._format_iso(job.get('dt_from'))} – {self._format_iso(job.get('dt_to'))}"
            self.detail_title_var.set(f"Pliki w zleceniu — sklep {job['shop']} ({job['city']}) {range_text}")
            self.selected_job_id = job_id
        else:
            self.detail_title_var.set('Pliki w zleceniu')
            self.selected_job_id = None

    @staticmethod
    def _format_iso(value: Optional[str]) -> str:
        if not value:
            return '-'
        try:
            return datetime.fromisoformat(value).strftime('%Y-%m-%d %H:%M')
        except Exception:
            return value

    @staticmethod
    def _format_remote(value: Optional[str]) -> str:
        if not value:
            return '-'
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
            try:
                return datetime.strptime(value, fmt).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                continue
        return value

    def on_job_select(self, _event=None):
        job_id = self._get_selected_job_id()
        self._populate_task_tree(job_id)

    def retry_selected_tasks(self):
        sel = self.task_tree.selection()
        if not sel:
            return
        job_ids = set()
        for iid in sel:
            if not iid.startswith('task_'):
                continue
            try:
                task_id = int(iid.split('_', 1)[1])
            except ValueError:
                continue
            task = self.db.get_task(task_id)
            if not task:
                continue
            self.db.update_task(task_id, status='pending', last_error=None, retries=0)
            if task.get('job_id'):
                job_ids.add(task['job_id'])
        for job_id in job_ids:
            self.db.refresh_job_status(job_id)
        self.refresh_tasks()

    def open_local_root(self):
        job_id = self._get_selected_job_id()
        target_path: Optional[Path] = None
        if job_id:
            job = self.db.get_job(job_id)
            if job and job.get('local_root'):
                target_path = Path(job['local_root'])
        if target_path is None:
            target_path = Path(self.local_root.get())
        if not target_path.exists():
            messagebox.showinfo('Info', f'Folder {target_path} jeszcze nie istnieje')
            return
        os.startfile(str(target_path))

# ----------------- Main -----------------
if __name__ == '__main__':
    db = DB(DB_PATH)
    app = tb.Window(themename='flatly')
    App(app, db)
    app.mainloop()
