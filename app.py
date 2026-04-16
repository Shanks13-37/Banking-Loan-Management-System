from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from banking_app import BankingService, init_db

ROOT_DIR = Path(__file__).resolve().parent
STATIC_DIR = ROOT_DIR / "static"
DATABASE_PATH = ROOT_DIR / "database" / "banking.db"


class BankingRequestHandler(SimpleHTTPRequestHandler):
    service = BankingService(DATABASE_PATH)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_GET(self) -> None:
        parsed_url = urlparse(self.path)

        if parsed_url.path == "/":
            self.path = "/index.html"
            return super().do_GET()

        if parsed_url.path == "/api/dashboard":
            return self._handle_api(lambda: self.service.dashboard_summary())

        if parsed_url.path == "/api/statements":
            query_params = parse_qs(parsed_url.query)
            account_id = int(query_params.get("account_id", ["0"])[0])
            limit = int(query_params.get("limit", ["25"])[0])
            return self._handle_api(lambda: self.service.get_account_statement(account_id, limit=limit))

        if parsed_url.path == "/api/views/defaulters":
            return self._handle_api(lambda: self.service.get_defaulters())

        if parsed_url.path == "/api/views/branch-performance":
            return self._handle_api(lambda: self.service.get_branch_performance())

        return super().do_GET()

    def do_POST(self) -> None:
        parsed_url = urlparse(self.path)
        payload = self._read_json_body()

        routes = {
            "/api/customers": lambda: self.service.create_customer(payload),
            "/api/accounts": lambda: self.service.create_account(payload),
            "/api/transfers": lambda: self.service.transfer_funds(payload),
            "/api/transfers/simulate-failure": lambda: self.service.simulate_failed_transfer(payload),
            "/api/loans": lambda: self.service.create_loan(payload),
            "/api/loans/approve": lambda: self.service.approve_loan(payload),
            "/api/emis/pay": lambda: self.service.pay_emi(payload),
            "/api/emis/run-cycle": lambda: self.service.run_emi_cycle(payload),
            "/api/concurrency-demo": lambda: self.service.simulate_concurrency(payload),
            "/api/reset-demo": lambda: self.service.reset_demo_data(),
        }

        handler = routes.get(parsed_url.path)
        if handler is None:
            self._send_json({"error": "API route not found."}, status=HTTPStatus.NOT_FOUND)
            return

        self._handle_api(handler)

    def _read_json_body(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length == 0:
            return {}

        raw_body = self.rfile.read(content_length).decode("utf-8")
        if not raw_body.strip():
            return {}
        return json.loads(raw_body)

    def _handle_api(self, func) -> None:
        try:
            response = func()
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except json.JSONDecodeError:
            self._send_json({"error": "Request body must be valid JSON."}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": "Internal server error", "details": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        self._send_json(response)

    def _send_json(self, payload: dict | list, *, status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Banking and Loan Management System")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument("--reset-db", action="store_true", help="Recreate the database with seed data before startup")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.reset_db or not DATABASE_PATH.exists():
        init_db(DATABASE_PATH, reset=args.reset_db)

    server = ThreadingHTTPServer((args.host, args.port), BankingRequestHandler)
    print(f"Banking & Loan Management System running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
