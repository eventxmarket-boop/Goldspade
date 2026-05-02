from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import json
import threading
import time
import random
import string

tickets = {}
ticket_lock = threading.Lock()

class ImprovedMockHandler(BaseHTTPRequestHandler):
    request_count = 0
    hit_lock = threading.Lock()

    def do_POST(self):
        path = self.path
        length = int(self.headers.get('Content-Length', 0) or 0)
        body = b''
        if length > 0:
            body = self.rfile.read(length)

        if path == '/work-orders':
            with ImprovedMockHandler.hit_lock:
                ImprovedMockHandler.request_count += 1
                current_count = ImprovedMockHandler.request_count
            tid = 'ticket_' + ''.join(random.choices(string.ascii_lowercase+string.digits, k=12))
            with ticket_lock:
                tickets[tid] = {'status': 'processing', 'start': time.time()}
            print(f'📝 MOCK POST /work-orders -> {tid}', flush=True)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'ticket_id': tid}).encode())

        elif path == '/mock_match':
            with ImprovedMockHandler.hit_lock:
                ImprovedMockHandler.request_count += 1
                current_count = ImprovedMockHandler.request_count
            print(f'🎯 MOCK POST /mock_match -> 200', flush=True)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            response = {
                "status": "success",
                "message": f"Slot secured! (Total hits: {current_count})"
            }
            self.wfile.write(json.dumps(response).encode())

        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        path = self.path
        if path.startswith('/work-orders/') and path.endswith('/status'):
            parts = path.split('/')
            tid = parts[2] if len(parts) >= 3 else ''
            with ticket_lock:
                if tid in tickets:
                    elapsed = time.time() - tickets[tid]['start']
                    if elapsed > 0.5:
                        tickets[tid]['status'] = 'completed'
                    status = tickets[tid]['status']
                else:
                    status = 'processing'
            print(f'🔍 MOCK GET {path} -> {status}', flush=True)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': status, 'resolution_data': 'mock_auth_token_xyz'}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

if __name__ == '__main__':
    server = ThreadingHTTPServer(('localhost', 9090), ImprovedMockHandler)
    print("🎯 Multi-threaded Advanced Mock Server running at http://localhost:9090")
    print("📡 支持 /work-orders + /work-orders/{id}/status + /mock_match")
    server.serve_forever()