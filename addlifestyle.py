import os
import re
import shutil
import platform
import threading
import requests
import tkinter as tk
from tkinter import filedialog, ttk
from PIL import Image, ImageTk
from io import BytesIO
import pandas as pd
from requests.adapters import HTTPAdapter

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

IS_WIN = platform.system() == "Windows"

if platform.system() == "Windows":
    SLIM_PARQUET = r"\\bruardb1\elucid\Program_Files\slim.parquet"
else:
    SLIM_PARQUET = "/Volumes/elucid/Program_Files/slim.parquet"
SMALL_BASE   = "https://www.houseofbruar.com/images/products/small/"
LARGE_BASE   = "https://www.houseofbruar.com/images/products/large/"

THUMB_W, THUMB_H = 160, 160
CELL_W           = THUMB_W + 22   # thumb + border allowance
CELL_H           = THUMB_H + 44   # thumb + label + padding
THUMB_PAD        = 6

C = {
    "bg":         "#f0f2f5",
    "sidebar":    "#4b5e78",
    "panel":      "#ffffff",
    "accent":     "#3b82f6",
    "accent_dk":  "#1d4ed8",
    "text":       "#111827",
    "subtle":     "#6b7280",
    "error":      "#dc2626",
    "border":     "#d1d5db",
    "btn_txt":    "#ffffff",
    "sb_text":    "#e8edf5",
    "sb_muted":   "#c5cfe0",
    "selected":   "#3b82f6",
    "card_bg":    "#dde3ed",
    "card_text":  "#1e3a5f",
    "warn_bg":    "#fff3cd",
    "warn_text":  "#856404",
}

FONT_UI         = ("Segoe UI", 10)           if IS_WIN else ("Helvetica Neue", 12)
FONT_MONO       = ("Consolas", 10)           if IS_WIN else ("Menlo", 11)
FONT_BOLD       = ("Segoe UI", 10, "bold")   if IS_WIN else ("Helvetica Neue", 12, "bold")
FONT_SMALL      = ("Segoe UI", 8)            if IS_WIN else ("Helvetica Neue", 10)
FONT_SMALL_BOLD = ("Segoe UI", 8, "bold")    if IS_WIN else ("Helvetica Neue", 10, "bold")

# ─────────────────────────────────────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_codes_from_folder(folder):
    """Return deduplicated base codes (strip _N suffix) from JPG filenames."""
    seen, codes = set(), []
    for fname in sorted(os.listdir(folder)):
        if fname.lower().endswith(".jpg"):
            base = os.path.splitext(fname)[0]
            code = re.sub(r"_\d+$", "", base).upper()
            if code and code not in seen:
                seen.add(code)
                codes.append(code)
    return codes


def load_slim(codes):
    """
    Read slim.parquet. Returns an ordered list of dicts, one per code found:
        { code, name, image_type, active }
    """
    if not os.path.exists(SLIM_PARQUET):
        raise FileNotFoundError(f"Cannot find slim.parquet at:\n{SLIM_PARQUET}")

    df = pd.read_parquet(SLIM_PARQUET, columns=["PF_ID", "Name", "image_type"])
    df["PF_ID"]      = df["PF_ID"].astype(str).str.upper().str.strip()
    df["image_type"] = df["image_type"].astype(str).str.strip()
    df["Name"]       = df["Name"].astype(str).str.strip()

    wanted = set(codes)
    subset = df[df["PF_ID"].isin(wanted)].drop_duplicates("PF_ID")
    lookup = subset.set_index("PF_ID")

    results = []
    for code in codes:
        if code in lookup.index:
            row = lookup.loc[code]
            results.append({
                "code":       code,
                "name":       row["Name"],
                "image_type": row["image_type"],
                "active":     row["image_type"] != "0",
            })
    return results


def probe_image_urls(session, code, base_url, cancel_event):
    """Return ordered list of image URLs that exist for the given code."""
    urls = []
    url0 = f"{base_url}{code}.jpg"
    try:
        if session.head(url0, timeout=8).status_code == 200:
            urls.append(url0)
        else:
            return urls
    except Exception:
        return urls
    i = 1
    while not cancel_event.is_set():
        url = f"{base_url}{code}_{i}.jpg"
        try:
            if session.head(url, timeout=8).status_code == 200:
                urls.append(url)
                i += 1
            else:
                break
        except Exception:
            break
    return urls


def download_bytes(session, url):
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            return r.content
    except Exception:
        pass
    return None


def make_thumb(img_bytes):
    img = Image.open(BytesIO(img_bytes)).convert("RGB")
    img.thumbnail((THUMB_W, THUMB_H), Image.LANCZOS)
    return ImageTk.PhotoImage(img)


def sorted_originals(folder):
    files = [f for f in os.listdir(folder) if f.lower().endswith(".jpg")]
    def sort_key(fname):
        m = re.search(r"_(\d+)$", os.path.splitext(fname)[0])
        return int(m.group(1)) if m else 0
    return sorted(files, key=sort_key)


def build_session():
    s = requests.Session()
    a = HTTPAdapter(pool_connections=20, pool_maxsize=20)
    s.mount("https://", a)
    s.mount("http://",  a)
    return s

# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Image Review")
        self.root.geometry("980x780")
        self.root.minsize(700, 560)
        self.root.configure(bg=C["bg"])

        self._setup_styles()

        # ── State ─────────────────────────────────────────────────────────────
        self.folder_var      = tk.StringVar()
        self.progress_var    = tk.IntVar(value=0)
        self.status_var      = tk.StringVar(value="Ready.")
        self.cancel_event    = threading.Event()

        self.thumb_refs      = []   # keep PhotoImage alive
        self.selected_urls   = set()
        self.thumb_cells     = []   # (frame, url) for all image cells
        self.downloaded_urls = []   # (url, code) in probe order

        # Dynamic-column grid state
        self._cols        = 5
        self._grid_rows   = []   # list of rows; each row = list of (widget, url|None)
        self._current_row = []   # row being assembled
        self._row_code    = None

        self._build_ui()
        self.canvas.bind("<Configure>", self._on_canvas_configure)

    # ── Styles ────────────────────────────────────────────────────────────────

    def _setup_styles(self):
        s = ttk.Style(self.root)
        s.theme_use("clam")
        s.configure(".", background=C["bg"], foreground=C["text"], font=FONT_UI)
        s.configure("TFrame", background=C["bg"])
        s.configure("TLabel", background=C["bg"], foreground=C["text"])

        s.configure("Primary.TButton",
            background=C["accent"], foreground=C["btn_txt"],
            font=FONT_BOLD, padding=(0, 10), relief="flat")
        s.map("Primary.TButton",
            background=[("active", C["accent_dk"]), ("disabled", C["border"])],
            foreground=[("disabled", C["subtle"])])

        s.configure("Secondary.TButton",
            background="#6b8099", foreground="#f0f4fa",
            font=FONT_UI, padding=(0, 8), relief="flat")
        s.map("Secondary.TButton",
            background=[("active", "#7a90aa"), ("disabled", "#7a90aa")],
            foreground=[("disabled", "#c0ccd8")])

        s.configure("Action.TButton",
            background="#16a34a", foreground="#ffffff",
            font=FONT_BOLD, padding=(0, 10), relief="flat")
        s.map("Action.TButton",
            background=[("active", "#15803d"), ("disabled", C["border"])],
            foreground=[("disabled", C["subtle"])])

        s.configure("Danger.TButton",
            background="#7f1d1d", foreground="#fca5a5",
            font=FONT_UI, padding=(0, 8), relief="flat")
        s.map("Danger.TButton",
            background=[("active", "#991b1b"), ("disabled", "#374151")],
            foreground=[("disabled", C["subtle"])])

        s.configure("TProgressbar",
            troughcolor=C["border"], background=C["accent"], thickness=6)

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        self.status_bar = tk.Label(
            self.root, textvariable=self.status_var, anchor=tk.W,
            bg=C["sidebar"], fg=C["sb_text"], font=FONT_UI, padx=10, pady=4)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        body = tk.Frame(self.root, bg=C["bg"])
        body.pack(fill=tk.BOTH, expand=True)
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=0)
        body.rowconfigure(0, weight=1)

        # ── Left content ──────────────────────────────────────────────────────
        content = tk.Frame(body, bg=C["bg"], padx=14, pady=12)
        content.grid(row=0, column=0, sticky="nsew")
        content.columnconfigure(0, weight=1)
        content.rowconfigure(3, weight=1)

        tk.Label(content, text="FOLDER", bg=C["bg"], fg=C["subtle"],
                 font=FONT_BOLD).grid(row=0, column=0, sticky="w", pady=(0, 4))

        folder_row = tk.Frame(content, bg=C["bg"])
        folder_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        folder_row.columnconfigure(0, weight=1)

        self.folder_entry = tk.Entry(
            folder_row, textvariable=self.folder_var,
            font=FONT_MONO, bg=C["panel"], fg=C["text"],
            relief="solid", bd=1, insertbackground=C["text"])
        self.folder_entry.grid(row=0, column=0, sticky="ew", ipady=5)
        ttk.Button(folder_row, text="Browse…", style="Secondary.TButton",
                   command=self._browse).grid(row=0, column=1, padx=(6, 0))

        # Drag-and-drop (requires tkinterdnd2)
        try:
            from tkinterdnd2 import DND_FILES
            self.folder_entry.drop_target_register(DND_FILES)
            self.folder_entry.dnd_bind("<<Drop>>", self._on_drop)
        except Exception:
            pass

        self.progress_bar = ttk.Progressbar(
            content, variable=self.progress_var, maximum=100, style="TProgressbar")
        self.progress_bar.grid(row=2, column=0, sticky="ew", pady=(0, 8))

        tk.Label(content, text="IMAGES — click to select / deselect",
                 bg=C["bg"], fg=C["subtle"],
                 font=FONT_BOLD).grid(row=2, column=0, sticky="w", pady=(0, 4))

        viewer_outer = tk.Frame(content, bg=C["border"], bd=1, relief="solid")
        viewer_outer.grid(row=3, column=0, sticky="nsew")
        viewer_outer.columnconfigure(0, weight=1)
        viewer_outer.rowconfigure(0, weight=1)

        self.canvas = tk.Canvas(viewer_outer, bg=C["panel"], highlightthickness=0)
        vsb = ttk.Scrollbar(viewer_outer, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=vsb.set)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        self.thumb_frame = tk.Frame(self.canvas, bg=C["panel"])
        self.canvas_win  = self.canvas.create_window(
            (0, 0), window=self.thumb_frame, anchor="nw")

        self.thumb_frame.bind("<Configure>", self._on_frame_configure)
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

        # ── Sidebar ───────────────────────────────────────────────────────────
        sidebar = tk.Frame(body, bg=C["sidebar"], width=140, padx=12, pady=16)
        sidebar.grid(row=0, column=1, sticky="nsew")
        sidebar.pack_propagate(False)

        def slabel(t):
            tk.Label(sidebar, text=t, bg=C["sidebar"], fg=C["sb_muted"],
                     font=("Segoe UI", 8, "bold") if IS_WIN
                     else ("Helvetica Neue", 9, "bold")
                     ).pack(fill=tk.X, pady=(14, 4))

        def sbtn(t, cmd, sty, state=tk.NORMAL):
            b = ttk.Button(sidebar, text=t, command=cmd, style=sty, state=state)
            b.pack(fill=tk.X, pady=(0, 6))
            return b

        slabel("RUN")
        self.start_btn  = sbtn("▶  Fetch Images", self._start,  "Primary.TButton")
        self.cancel_btn = sbtn("✕  Cancel",        self._cancel, "Danger.TButton",
                               state=tk.DISABLED)

        slabel("TOOLS")
        sbtn("Clear", self._clear_all, "Secondary.TButton")

        slabel("CONFIRM")
        self.download_btn = sbtn("⬇  Download\n   Selected",
                                 self._download_selected, "Action.TButton",
                                 state=tk.DISABLED)

    # ── Canvas / scroll ───────────────────────────────────────────────────────

    def _on_frame_configure(self, _e):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        self.canvas.itemconfig(self.canvas_win, width=event.width)
        new_cols = max(2, (event.width - THUMB_PAD) // (CELL_W + THUMB_PAD))
        if new_cols != self._cols:
            self._cols = new_cols
            self._re_grid()

    def _on_mousewheel(self, event):
        delta = event.delta
        # On Mac, delta is already in small units (no /120 needed)
        if IS_WIN:
            delta = delta // 120
        self.canvas.yview_scroll(int(-1 * delta), "units")

    # ── Drag & drop ───────────────────────────────────────────────────────────

    def _on_drop(self, event):
        raw  = event.data.strip()
        path = re.split(r"\s+(?=[A-Za-z]:\\|\{)", raw)[0].strip("{}")
        if os.path.isdir(path):
            self.folder_var.set(path)

    def _browse(self):
        chosen = filedialog.askdirectory()
        if chosen:
            self.folder_var.set(chosen)

    # ── Status ────────────────────────────────────────────────────────────────

    def _set_status(self, msg, error=False):
        self.status_var.set(msg)
        self.status_bar.config(fg=C["error"] if error else C["sb_text"])
        self.root.update_idletasks()

    def _set_busy(self, busy):
        self.start_btn.config( state=tk.DISABLED if busy else tk.NORMAL)
        self.cancel_btn.config(state=tk.NORMAL   if busy else tk.DISABLED)

    # ── Clear ─────────────────────────────────────────────────────────────────

    def _clear_all(self):
        self._clear_viewer()
        self.folder_var.set("")
        self.progress_var.set(0)
        self.downloaded_urls.clear()
        self.download_btn.config(state=tk.DISABLED)
        self._set_status("Ready.")

    def _clear_viewer(self):
        for w in self.thumb_frame.winfo_children():
            w.destroy()
        self.thumb_refs.clear()
        self.selected_urls.clear()
        self.thumb_cells.clear()
        self._grid_rows.clear()
        self._current_row.clear()
        self._row_code = None

    # ── Dynamic grid ─────────────────────────────────────────────────────────

    def _re_grid(self):
        """Re-place every widget when column count changes."""
        for row_idx, row in enumerate(self._grid_rows):
            if len(row) == 1:
                # Banner row (no-lifestyle) spans full width
                widget, _ = row[0]
                widget.grid(row=row_idx, column=0,
                            columnspan=self._cols,
                            padx=THUMB_PAD, pady=(4, 2), sticky="ew")
            else:
                for col_idx, (widget, _) in enumerate(row):
                    widget.grid(row=row_idx, column=col_idx,
                                padx=THUMB_PAD, pady=THUMB_PAD, sticky="n")

    def _flush_current_row(self):
        """Commit the row being built to the grid."""
        if not self._current_row:
            return
        row_idx = len(self._grid_rows)
        self._grid_rows.append(list(self._current_row))
        for col_idx, (widget, _) in enumerate(self._current_row):
            widget.grid(row=row_idx, column=col_idx,
                        padx=THUMB_PAD, pady=THUMB_PAD, sticky="n")
        self._current_row = []

    def _begin_product_row(self, info):
        """Start a new product row with its info card in column 0."""
        self._flush_current_row()
        self._row_code = info["code"]
        card = self._make_info_card(info)
        self._current_row = [(card, None)]

    def _add_cell_to_current_row(self, widget, url, row_code=None):
        """Append a thumb cell; wrap to a plain new row if full."""
        if len(self._current_row) >= self._cols:
            self._flush_current_row()
            self._current_row = []

        self._current_row.append((widget, url))
        self.thumb_cells.append((widget, url))

    # ── Info card ─────────────────────────────────────────────────────────────

    def _make_info_card(self, info):
        bg = C["card_bg"]
        fg = C["card_text"]

        card = tk.Frame(self.thumb_frame, bg=bg,
                        width=CELL_W, height=CELL_H,
                        highlightthickness=1,
                        highlightbackground=C["border"])
        card.pack_propagate(False)
        card.grid_propagate(False)

        inner = tk.Frame(card, bg=bg)
        inner.pack(anchor="nw", fill=tk.BOTH, expand=True, padx=6, pady=6)

        tk.Label(inner, text=info["code"], bg=bg, fg=fg,
                 font=FONT_SMALL_BOLD, anchor="w").pack(anchor="w")
        tk.Label(inner, text=info["name"], bg=bg, fg=fg,
                 font=FONT_SMALL, wraplength=CELL_W - 16,
                 justify="left", anchor="w").pack(anchor="w", pady=(1, 3))
        tk.Label(inner, text=f"Type: {info['image_type']}", bg=bg, fg=fg,
                 font=FONT_SMALL, anchor="w").pack(anchor="w")

        return card

    def _make_no_lifestyle_banner(self, info):
        """Single-line full-width banner for inactive (image_type=0) products."""
        bg  = C["warn_bg"]
        fg  = C["warn_text"]
        txt = f"{info['code']}  —  {info['name']}  —  No Lifestyle"

        banner = tk.Frame(self.thumb_frame, bg=bg,
                          highlightthickness=1,
                          highlightbackground="#e6c84a")
        tk.Label(banner, text=txt, bg=bg, fg=fg,
                 font=FONT_SMALL_BOLD,
                 anchor="w", padx=8, pady=5).pack(fill=tk.X)
        return banner

    # ── Thumb cell ────────────────────────────────────────────────────────────

    def _make_thumb_cell(self, img_bytes, url, auto_select=False):
        try:
            photo = make_thumb(img_bytes)
        except Exception:
            return None

        self.thumb_refs.append(photo)

        outer = tk.Frame(self.thumb_frame, bg=C["panel"],
                         highlightthickness=3,
                         highlightbackground=C["border"],
                         highlightcolor=C["border"],
                         cursor="hand2")

        img_lbl = tk.Label(outer, image=photo, bg=C["panel"], cursor="hand2")
        img_lbl.pack()

        tk.Label(outer, text=url.split("/")[-1], bg=C["panel"], fg=C["subtle"],
                 font=FONT_SMALL, wraplength=THUMB_W).pack(pady=(2, 4))

        def toggle(event=None, u=url, f=outer):
            self._toggle_selection(u, f)

        outer.bind("<Button-1>",   toggle)
        img_lbl.bind("<Button-1>", toggle)

        if auto_select:
            self.selected_urls.add(url)
            outer.config(highlightbackground=C["selected"],
                         highlightcolor=C["selected"])

        return outer

    # ── Selection ─────────────────────────────────────────────────────────────

    def _toggle_selection(self, url, frame):
        if url in self.selected_urls:
            self.selected_urls.discard(url)
            frame.config(highlightbackground=C["border"],
                         highlightcolor=C["border"])
        else:
            self.selected_urls.add(url)
            frame.config(highlightbackground=C["selected"],
                         highlightcolor=C["selected"])

    # ── Cancel ────────────────────────────────────────────────────────────────

    def _cancel(self):
        self.cancel_event.set()
        self._set_status("Cancelling…")

    # ── Fetch worker ──────────────────────────────────────────────────────────

    def _start(self):
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            self._set_status("Please select a valid folder.", error=True)
            return

        self.cancel_event.clear()
        self._set_busy(True)
        self.download_btn.config(state=tk.DISABLED)
        self._clear_viewer()
        self.progress_var.set(0)
        self.downloaded_urls.clear()

        threading.Thread(target=self._fetch_worker, args=(folder,), daemon=True).start()

    def _fetch_worker(self, folder):
        try:
            codes = get_codes_from_folder(folder)
            if not codes:
                self.root.after(0, lambda: self._set_status(
                    "No JPG files found in folder.", error=True))
                return

            self.root.after(0, lambda: self._set_status(
                f"Loading slim.parquet for {len(codes)} code(s)…"))

            try:
                products = load_slim(codes)
            except FileNotFoundError as e:
                self.root.after(0, lambda: self._set_status(str(e), error=True))
                return

            if not products:
                self.root.after(0, lambda: self._set_status(
                    "No matching codes found in slim.parquet.", error=True))
                return

            session = build_session()

            # Probe all URLs first so we know the total download count
            self.root.after(0, lambda: self._set_status("Probing image URLs…"))
            probe_results = []
            for prod in products:
                if self.cancel_event.is_set():
                    break
                urls = (probe_image_urls(session, prod["code"], SMALL_BASE,
                                         self.cancel_event)
                        if prod["active"] else [])
                probe_results.append((prod, urls))

            total = sum(len(u) for _, u in probe_results)
            done  = 0
            any_images = False

            self.root.after(0, lambda: self._set_status(
                f"Downloading {total} preview(s)…"))

            for prod, urls in probe_results:
                if self.cancel_event.is_set():
                    break

                _prod = prod

                if not urls:
                    # Inactive product or no images found: show info card only
                    self.root.after(0, lambda p=_prod: self._add_solo_card(p))
                    continue

                # Start a new product row
                self.root.after(0, lambda p=_prod: self._begin_product_row(p))

                first = True
                for url in urls:
                    if self.cancel_event.is_set():
                        break
                    img_bytes = download_bytes(session, url)
                    done += 1
                    pct = int(done / total * 100) if total else 100

                    if img_bytes:
                        any_images = True
                        self.downloaded_urls.append((url, prod["code"]))
                        _b, _u, _f, _c = img_bytes, url, first, prod["code"]
                        self.root.after(0,
                            lambda b=_b, u=_u, f=_f, c=_c: self._append_thumb(b, u, f, c))
                        first = False

                    self.root.after(0, lambda p=pct, d=done, t=total:
                                    (self.progress_var.set(p),
                                     self._set_status(
                                         f"Downloaded {d} of {t} previews…")))

            self.root.after(0, self._flush_current_row)

            if self.cancel_event.is_set():
                self.root.after(0, lambda: self._set_status("Cancelled."))
            else:
                msg = (f"Done — {len(self.downloaded_urls)} preview(s) ready. "
                       "Select images then click Download Selected."
                       if any_images else
                       "Done — no images found online for these codes.")
                self.root.after(0, lambda: self._set_status(msg))
                if any_images:
                    self.root.after(0, lambda: self.download_btn.config(
                        state=tk.NORMAL))

        except Exception as e:
            self.root.after(0, lambda: self._set_status(str(e), error=True))
        finally:
            self.root.after(0, lambda: self._set_busy(False))

    # ── Viewer additions (always called on main thread) ───────────────────────

    def _add_solo_card(self, info):
        """
        Full-width banner row for inactive products (image_type = 0).
        Spans all columns so it reads as a single line across the viewer.
        """
        self._flush_current_row()
        banner  = self._make_no_lifestyle_banner(info)
        row_idx = len(self._grid_rows)
        self._grid_rows.append([(banner, None)])
        banner.grid(row=row_idx, column=0,
                    columnspan=self._cols,
                    padx=THUMB_PAD, pady=(4, 2), sticky="ew")

    def _append_thumb(self, img_bytes, url, auto_select=False, row_code=""):
        cell = self._make_thumb_cell(img_bytes, url, auto_select=auto_select)
        if cell:
            self._add_cell_to_current_row(cell, url, row_code)

    # ── Finalise download ─────────────────────────────────────────────────────

    def _download_selected(self):
        if not self.selected_urls:
            self._set_status("No images selected.", error=True)
            return
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            self._set_status("Original folder no longer valid.", error=True)
            return
        self.download_btn.config(state=tk.DISABLED)
        self.start_btn.config(state=tk.DISABLED)
        self._set_status("Downloading large images and building output folder…")
        threading.Thread(target=self._finalize_worker, args=(folder,), daemon=True).start()

    def _finalize_worker(self, folder):
        try:
            folder_name   = os.path.basename(folder.rstrip("/\\"))
            output_folder = os.path.join(
                os.path.dirname(folder.rstrip("/\\")),
                f"{folder_name} Edited")
            os.makedirs(output_folder, exist_ok=True)

            session = build_session()

            selected_ordered = [(u, c) for u, c in self.downloaded_urls
                                if u in self.selected_urls]

            code_new_counts = {}
            for _, code in selected_ordered:
                code_new_counts[code] = code_new_counts.get(code, 0) + 1

            total_dl    = len(selected_ordered)
            done        = 0
            code_dl_idx = {}

            # 1. Download large versions
            for url, code in selected_ordered:
                large_url = url.replace(SMALL_BASE, LARGE_BASE)
                img_bytes = download_bytes(session, large_url)
                idx       = code_dl_idx.get(code, 0)
                out_name  = f"{code}.jpg" if idx == 0 else f"{code}_{idx}.jpg"
                code_dl_idx[code] = idx + 1

                if img_bytes:
                    with open(os.path.join(output_folder, out_name), "wb") as f:
                        f.write(img_bytes)

                done += 1
                pct   = int(done / (total_dl + 1) * 100)
                self.root.after(0, lambda p=pct, d=done, t=total_dl:
                                (self.progress_var.set(p),
                                 self._set_status(
                                     f"Downloading large image {d} of {t}…")))

            # 2. Copy & rename originals
            orig_by_code = {}
            for fname in sorted_originals(folder):
                code = re.sub(r"_\d+$", "", os.path.splitext(fname)[0]).upper()
                orig_by_code.setdefault(code, []).append(fname)

            for code, files in orig_by_code.items():
                new_count = code_new_counts.get(code, 0)
                for i, fname in enumerate(files):
                    suf      = new_count + i
                    new_name = f"{code}.jpg" if suf == 0 else f"{code}_{suf}.jpg"
                    shutil.copy2(os.path.join(folder, fname),
                                 os.path.join(output_folder, new_name))

            self.root.after(0, lambda: self.progress_var.set(100))
            self.root.after(0, lambda: self._set_status(
                f"Complete! Output saved to: {output_folder}"))

            if IS_WIN:
                os.startfile(output_folder)
            elif platform.system() == "Darwin":
                import subprocess
                subprocess.Popen(["open", output_folder])

        except Exception as e:
            self.root.after(0, lambda: self._set_status(str(e), error=True))
        finally:
            self.root.after(0, lambda: self.start_btn.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.download_btn.config(state=tk.NORMAL))


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        from tkinterdnd2 import TkinterDnD
        root = TkinterDnD.Tk()
    except ImportError:
        root = tk.Tk()

    app = App(root)
    root.mainloop()