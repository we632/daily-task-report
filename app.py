from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

import pandas as pd
import io
import uuid
import time
import zipfile
import os
import tempfile
import asyncio
from contextlib import suppress


from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont


# ===== PDF 中文字体（最稳方案，无需字体文件）=====
pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
PDF_FONT = "STSong-Light"


app = FastAPI()
async def cleanup_loop():
    while True:
        cleanup_store()
        await asyncio.sleep(10 * 60)  # 每10分钟清一次，可调

templates = Jinja2Templates(directory="templates")

# ============ 增强版配置（支持别名匹配） ============
# Key 是代码内部使用的“标准名”，Value 是 Excel 中可能出现的表头别名
COLUMN_ALIASES = {
    "运单号": ["运单号", "Tracking No.", "Tracking Number", "TrackingNo", "Waybill"],
    "DSP名称": ["DSP名称", "DSP", "DSP Name", "dsp_name", "最后一次操作DSP", "所属DSP"],
    "区域名称": ["区域名称", "区域", "Area", "Zone", "分拣代码(3段码格式)"],
    "司机名称": ["司机名称", "name", "Delivery Driver", "Driver Name", "Driver", "最后一次操作司机", "最后扫码司机"],
    "任务日期": ["任务日期", "日期", "Task Date", "Date", "派件扫描时间"],
    "运单状态": ["运单状态", "状态", "Status", "订单状态"],
    "仓库名称": ["仓库名称", "仓库", "Warehouse", "WH"],
}

# 这样可以确保无论 Excel 里叫什么，网页上显示的永远是列表里的第一个英文名
COLUMN_MAP = {
    "运单号": "Tracking No.",
    "DSP名称": "DSP Name",
    "区域名称": "Area",
    "司机名称": "Driver",
    "任务日期": "Date",
    "运单状态": "Status",
    "仓库名称": "Warehouse",
}

# 默认选中的列（使用标准名）
DEFAULT_COLUMNS = ["运单号", "DSP名称", "区域名称", "司机名称", "任务日期", "运单状态"]

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    扫描 Excel 列名，如果匹配到别名，则统一重命名为标准中文名
    """
    rename_dict = {}
    for col in df.columns:
        col_str = str(col).strip()
        for standard_name, aliases in COLUMN_ALIASES.items():
            if col_str in aliases:
                rename_dict[col] = standard_name
                break
    return df.rename(columns=rename_dict)


def normalize_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    统一字段值格式：
    - 区域名称：若是 3 段码（如 EWR-PHL-B11），只保留最后一段（B11）。
    """
    if "区域名称" in df.columns:
        area_series = df["区域名称"].fillna("").astype(str).str.strip()
        df["区域名称"] = area_series.str.split("-").str[-1].str.strip()
    return df

# 上传文件内存缓存：file_id -> {"bytes":..., "ts":...}
# 临时文件缓存：file_id -> {"path":..., "ts":..., "size":...}
FILE_STORE: dict[str, dict] = {}
FILE_TTL_SECONDS = 60 * 60  # 1小时


def cleanup_store() -> None:
    now = time.time()
    expired = [k for k, v in FILE_STORE.items() if now - v["ts"] > FILE_TTL_SECONDS]
    for k in expired:
        info = FILE_STORE.pop(k, None)
        if not info:
            continue
        path = info.get("path")
        if path and os.path.exists(path):
            with suppress(Exception):
                os.remove(path)

def save_file(content: bytes) -> str:
    cleanup_store()
    file_id = str(uuid.uuid4())

    # ✅ 写入系统临时目录（跨平台）
    fd, path = tempfile.mkstemp(prefix="wms_", suffix=".xlsx")
    os.close(fd)  # 只要路径，写入用 open

    with open(path, "wb") as f:
        f.write(content)

    FILE_STORE[file_id] = {"path": path, "ts": time.time(), "size": len(content)}
    return file_id

def load_file(file_id: str) -> bytes:
    cleanup_store()
    if not file_id or file_id not in FILE_STORE:
        raise ValueError("file_id invalid or expired, please upload again.")
    path = FILE_STORE[file_id]["path"]
    if not path or not os.path.exists(path):
        # 文件被删/丢失
        FILE_STORE.pop(file_id, None)
        raise ValueError("temp file missing, please upload again.")
    with open(path, "rb") as f:
        return f.read()


def read_excel(upload_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(upload_bytes), sheet_name=0)
    
    # --- 新增这一行 ---
    df = normalize_columns(df)
    df = normalize_values(df)
    # -----------------

    df = df.dropna(how="all")
    df = df.fillna("")
    df = df.loc[~(df.astype(str).apply(lambda r: (r.str.strip() == "").all(), axis=1))]
    return df


def read_combined_excels(file_ids: list[str]) -> pd.DataFrame:
    if not file_ids:
        raise ValueError("No files provided.")

    frames = []
    for fid in file_ids:
        content = load_file(fid)
        frames.append(read_excel(content))

    if not frames:
        raise ValueError("No valid files provided.")

    return pd.concat(frames, ignore_index=True)


def unique_sorted(df: pd.DataFrame, col: str) -> list[str]:
    if col not in df.columns:
        return []
    s = df[col].dropna().astype(str).str.strip()
    return sorted(x for x in s.unique().tolist() if x != "")

def apply_filters(
    df: pd.DataFrame,
    dsps: list[str],
    areas: list[str],
    drivers: list[str],
    statuses: list[str],
) -> pd.DataFrame:
    if dsps and "DSP名称" in df.columns:
        df = df[df["DSP名称"].isin(dsps)]
    if areas and "区域名称" in df.columns:
        df = df[df["区域名称"].isin(areas)]
    if drivers and "司机名称" in df.columns:
        df = df[df["司机名称"].isin(drivers)]
    if statuses and "运单状态" in df.columns:
        df = df[df["运单状态"].isin(statuses)]
    return df

def select_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    cols = cols or DEFAULT_COLUMNS
    cols = [c for c in cols if c in df.columns]
    return df[cols].copy()




def get_zip_compression_method() -> int:
    """
    在运行环境不支持 zlib/DEFLATED 时，回退到 ZIP_STORED，避免导出崩溃。
    """
    try:
        import zlib  # noqa: F401
        return zipfile.ZIP_DEFLATED
    except Exception:
        return zipfile.ZIP_STORED


def build_report_date_label(df: pd.DataFrame) -> tuple[str, str]:
    """
    从筛选后的数据里提取任务日期，生成可用于标题/文件名的日期标签。
    """
    if "任务日期" not in df.columns:
        return "unknown-date", "unknown-date"

    dates = (
        df["任务日期"]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    if not dates:
        return "unknown-date", "unknown-date"

    parsed_dates = pd.to_datetime(dates, errors="coerce")

    if parsed_dates.notna().any():
        valid = parsed_dates.dropna()
        start = valid.min()
        end = valid.max()

        start_display = f"{start.month}/{start.day}"
        end_display = f"{end.month}/{end.day}"
        display = start_display if start == end else f"{start_display}-{end_display}"
        return display, display.replace("/", "-")

    dates = sorted(dates)
    if len(dates) == 1:
        return dates[0], dates[0].replace("/", "-")

    display = f"{dates[0]}-{dates[-1]}"
    return display, display.replace("/", "-")


@app.on_event("startup")
async def _startup():
    asyncio.create_task(cleanup_loop())


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "column_map": COLUMN_MAP,
            "default_columns": DEFAULT_COLUMNS,
        },
    )


@app.post("/preview", response_class=HTMLResponse)
async def preview(
    request: Request,
    files: list[UploadFile] | None = File(None),
    file_ids: list[str] = Form([]),
    old_file_ids: list[str] = Form([]),
    # 列选择
    selected_columns: list[str] = Form([]),

    # 4个多选筛选
    selected_dsps: list[str] = Form([]),
    selected_areas: list[str] = Form([]),
    selected_drivers: list[str] = Form([]),
    selected_statuses: list[str] = Form([]),
):
    # 1) 读取文件（首次上传 or 后续用 file_id）
# 1) 读取文件（首次上传 or 后续用 file_id）
    if files:
        MAX_UPLOAD_MB = 20
        MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

        uploaded_file_ids = []
        for file in files:
            content = await file.read()
            if not content:
                continue
            if len(content) > MAX_UPLOAD_BYTES:
                raise ValueError(f"File too large (>{MAX_UPLOAD_MB}MB).")
            uploaded_file_ids.append(save_file(content))

        if not uploaded_file_ids:
            raise ValueError("No valid file uploaded.")

        # ✅ 再删除旧 file（只在“上传新文件”时做）
        for old_file_id in old_file_ids:
            if old_file_id and old_file_id in FILE_STORE:
                info = FILE_STORE.pop(old_file_id, None)
                if info:
                    path = info.get("path")
                    if path and os.path.exists(path):
                        with suppress(Exception):
                            os.remove(path)

        active_file_ids = uploaded_file_ids

    else:
        if not file_ids:
            raise ValueError("No file or file_ids provided.")
        active_file_ids = file_ids

    # 2) 读取 Excel
    df = read_combined_excels(active_file_ids)
    df["_orig_index"] = df.index.astype(int)

    # 3) 生成筛选选项
    dsp_options = unique_sorted(df, "DSP名称")
    area_options = unique_sorted(df, "区域名称")
    driver_options = unique_sorted(df, "司机名称")
    status_options = unique_sorted(df, "运单状态")

    # 4) 应用筛选
    fdf = apply_filters(
        df,
        selected_dsps,
        selected_areas,
        selected_drivers,
        selected_statuses,
    )

    # 5) 分组展示
    grouped = []
    show_cols = selected_columns or DEFAULT_COLUMNS

    if "DSP名称" in fdf.columns:
        for dsp, g in fdf.groupby("DSP名称", dropna=False):
            g_show = select_columns(g, show_cols)
            rows = [
                [int(idx)] + row
                for idx, row in zip(g["_orig_index"], g_show.values.tolist())
            ]
            grouped.append((dsp, rows))
    else:
        g_show = select_columns(fdf, show_cols)
        rows = [
            [int(idx)] + row
            for idx, row in zip(fdf["_orig_index"], g_show.values.tolist())
        ]
        grouped.append(("ALL", rows))

    display_headers = [
        COLUMN_MAP.get(c, c) for c in show_cols if c in fdf.columns
    ]

    return templates.TemplateResponse(
        "report.html",
        {
            "request": request,
            "file_ids": active_file_ids,
            "row_count": len(fdf),

            "grouped": grouped,
            "headers": display_headers,

            "column_map": COLUMN_MAP,
            "selected_columns": show_cols,

            "dsp_options": dsp_options,
            "area_options": area_options,
            "driver_options": driver_options,
            "status_options": status_options,

            "selected_dsps": selected_dsps,
            "selected_areas": selected_areas,
            "selected_drivers": selected_drivers,
            "selected_statuses": selected_statuses,
        },
    )

@app.post("/export/excel")
def export_excel(
    file_ids: list[str] = Form([]),
    selected_columns: list[str] = Form([]),
    selected_dsps: list[str] = Form([]),
    selected_areas: list[str] = Form([]),
    selected_drivers: list[str] = Form([]),
    selected_statuses: list[str] = Form([]),
):
    df = read_combined_excels(file_ids)

    fdf = apply_filters(df, selected_dsps, selected_areas, selected_drivers, selected_statuses)
    fdf = select_columns(fdf, selected_columns or DEFAULT_COLUMNS)

    wb = Workbook()
    ws = wb.active
    ws.title = "Report"

    headers = [COLUMN_MAP.get(c, c) for c in fdf.columns]
    ws.append(headers)

    for row in fdf.values.tolist():
        ws.append(row)

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)

    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=report.xlsx"},
    )
@app.post("/export/pdf")
def export_pdf(
    file_ids: list[str] = Form([]),
    selected_columns: list[str] = Form([]),
    selected_dsps: list[str] = Form([]),
    selected_areas: list[str] = Form([]),
    selected_drivers: list[str] = Form([]),
    selected_statuses: list[str] = Form([]),
):
    df = read_combined_excels(file_ids)

    fdf = apply_filters(df, selected_dsps, selected_areas, selected_drivers, selected_statuses)
    fdf = select_columns(fdf, selected_columns or DEFAULT_COLUMNS)

    headers = [COLUMN_MAP.get(c, c) for c in fdf.columns]

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(letter),
        leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18
    )
    styles = getSampleStyleSheet()
    title_style = styles["Heading3"]
    title_style.fontName = PDF_FONT

    story = []
    groups = fdf.groupby("DSP名称", dropna=False) if "DSP名称" in fdf.columns else [("ALL", fdf)]

    for dsp, g in groups:
        story.append(Paragraph(f"DSP: {dsp}", title_style))
        story.append(Spacer(1, 6))

        data = [headers] + g.values.tolist()
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), PDF_FONT),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.whitesmoke]),
        ]))

        story.append(table)
        story.append(PageBreak())

    doc.build(story)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=report.pdf"},
    )

@app.post("/export/pdf_zip")
def export_pdf_zip(
    file_ids: list[str] = Form([]),
    selected_columns: list[str] = Form([]),
    selected_dsps: list[str] = Form([]),
    selected_areas: list[str] = Form([]),
    selected_drivers: list[str] = Form([]),
    selected_statuses: list[str] = Form([]),
):
    df = read_combined_excels(file_ids)

    filtered_df = apply_filters(df, selected_dsps, selected_areas, selected_drivers, selected_statuses)
    report_date_display, report_date_safe = build_report_date_label(filtered_df)

    if "DSP名称" in filtered_df.columns:
        dsp_date_labels = {
            dsp: build_report_date_label(group_df)
            for dsp, group_df in filtered_df.groupby("DSP名称", dropna=False)
        }
    else:
        dsp_date_labels = {"ALL": build_report_date_label(filtered_df)}

    fdf = select_columns(filtered_df, selected_columns or DEFAULT_COLUMNS)
    headers = [COLUMN_MAP.get(c, c) for c in fdf.columns]

    groups = (
        fdf.groupby("DSP名称", dropna=False)
        if "DSP名称" in fdf.columns
        else [("ALL", fdf)]
    )

    def safe_filename(name: str) -> str:
        s = str(name or "DSP")
        s = s.encode("ascii", "ignore").decode("ascii") or "DSP"
        for ch in ['\\','/',':','*','?','"','<','>','|']:
            s = s.replace(ch, "_")
        return s[:60]

    def set_pdf_meta(title: str):
        def _cb(canvas, doc):
            canvas.setTitle(title)
            canvas.setAuthor("WMS Report")
        return _cb

    def build_zip_bytes(compression_method: int) -> bytes:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", compression=compression_method) as zf:
            styles = getSampleStyleSheet()
            title_style = styles["Heading3"]
            title_style.fontName = PDF_FONT

            for dsp, g in groups:
                buf = io.BytesIO()
                doc = SimpleDocTemplate(
                    buf,
                    pagesize=landscape(letter),
                    leftMargin=18, rightMargin=18,
                    topMargin=18, bottomMargin=18
                )

                dsp_date_display, dsp_date_safe = dsp_date_labels.get(
                    dsp, (report_date_display, report_date_safe)
                )

                story = []
                story.append(Paragraph(f"DSP: {dsp} | Date: {dsp_date_display}", title_style))
                story.append(Spacer(1, 6))

                data = [headers] + g.values.tolist()
                table = Table(data, repeatRows=1)
                table.setStyle(TableStyle([
                    ("FONTNAME", (0, 0), (-1, -1), PDF_FONT),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ]))
                story.append(table)

                report_title = f"DSP: {dsp} | Date: {dsp_date_display}"
                doc.build(
                    story,
                    onFirstPage=set_pdf_meta(report_title),
                    onLaterPages=set_pdf_meta(report_title)
                )

                buf.seek(0)
                zf.writestr(
                    f"{safe_filename(dsp)}_{safe_filename(dsp_date_safe)}.pdf",
                    buf.read(),
                )

        zip_buf.seek(0)
        return zip_buf.read()

    compression_method = get_zip_compression_method()
    try:
        zip_bytes = build_zip_bytes(compression_method)
    except NotImplementedError:
        zip_bytes = build_zip_bytes(zipfile.ZIP_STORED)

    zip_buf = io.BytesIO(zip_bytes)

    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=reports_by_dsp_{report_date_safe}.zip"},
    )
