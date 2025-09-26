import os
import gspread
from google.oauth2.service_account import Credentials
from typing import Dict, Optional, List

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
WORKSHEET_GID = os.getenv("WORKSHEET_GID", "").strip()  # numeric string, e.g. "1778352903"
CODE_HEADER = os.getenv("CODE_HEADER", "Код товара").strip()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

class SheetsClient:
    def _init_(self):
        svc_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not svc_json:
            raise RuntimeError("Missing env GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON).")
        credentials = Credentials.from_service_account_info(
            eval(svc_json) if svc_json.strip().startswith("{") else {}
        ).with_scopes(SCOPES)

        self.gc = gspread.authorize(credentials)
        if not GOOGLE_SHEET_ID:
            raise RuntimeError("Missing env GOOGLE_SHEET_ID.")

        self.sheet = self.gc.open_by_key(GOOGLE_SHEET_ID)
        self.ws = self._get_worksheet()

        # cache headers
        self.headers = self._read_headers()
        self.header_to_col = {h.lower(): idx+1 for idx, h in enumerate(self.headers)}

        if CODE_HEADER.lower() not in self.header_to_col:
            raise RuntimeError(
                f"Столбец с кодом не найден. Укажите правильный CODE_HEADER (сейчас '{CODE_HEADER}'). "
                f"Текущие заголовки: {self.headers}"
            )

    def _get_worksheet(self):
        if WORKSHEET_GID:
            ws = self.sheet.worksheet_by_id(int(WORKSHEET_GID))
            if ws:
                return ws
        # fallback: первая вкладка
        return self.sheet.get_worksheet(0)

    def _read_headers(self) -> List[str]:
        row = self.ws.row_values(1)
        return [h.strip() for h in row]

    def _ensure_headers(self, keys: List[str]):
        changed = False
        for k in keys:
            if k.lower() not in self.header_to_col:
                # add new header at the end
                self.headers.append(k)
                self.header_to_col[k.lower()] = len(self.headers)
                changed = True
        if changed:
            self.ws.update(range_name=gspread.utils.rowcol_to_a1(1,1),
                           values=[self.headers])

    def _find_row_by_code(self, code: str) -> Optional[int]:
        code_col_idx = self.header_to_col[CODE_HEADER.lower()]
        col_values = self.ws.col_values(code_col_idx)[1:]  # skip header
        for i, v in enumerate(col_values, start=2):
            if str(v).strip().lower() == str(code).strip().lower():
                return i
        return None

    def upsert_by_code(self, code: str, data: Dict[str, str]) -> int:
        """
        Возвращает номер строки. Обновляет или добавляет.
        data — словарь вида {"Цена": "36500", "Город": "Алматы"}
        """
        # убедимся, что заголовок с кодом точно есть
        self._ensure_headers([CODE_HEADER] + list(data.keys()))

        row_idx = self._find_row_by_code(code)
        if row_idx is None:
            # append new row
            row_idx = len(self.ws.get_all_values()) + 1
            # гарантируем длину ряда
            row_vals = [""] * len(self.headers)
            # проставим код
            row_vals[self.header_to_col[CODE_HEADER.lower()] - 1] = code
            self.ws.update(gspread.utils.rowcol_to_a1(row_idx, 1), [row_vals])

        # обновляем поля
        updates = []
        for k, v in data.items():
            col_idx = self.header_to_col[k.lower()]
            updates.append({
                "range": gspread.utils.rowcol_to_a1(row_idx, col_idx),
                "values": [[str(v)]],
            })

        # пакетное обновление
        self.ws.batch_update([{"range": u["range"], "values": u["values"]} for u in updates])
        return row_idx
