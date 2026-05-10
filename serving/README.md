# Web app

Two-tier app: FastAPI backend + Next.js frontend, both deployed in k8s.

```
serving/
├── backend/      FastAPI · 4 files · reads StarRocks + Stockfish + Vertex AI
└── frontend/     Next.js 14 · TypeScript · Tailwind · chessground · recharts
```

## Build & deploy

Run on the VPS (where Docker + k3s live):

```bash
cd /path/to/lichess-platform
git pull

# Backend image
docker build -t vietnoy/chess-webapp-backend:latest serving/backend
docker push vietnoy/chess-webapp-backend:latest

# Frontend image
docker build -t vietnoy/chess-webapp-frontend:latest serving/frontend
docker push vietnoy/chess-webapp-frontend:latest

# Apply manifests (first time only)
kubectl apply -f infra/k8s/webapp.yaml

# Roll out new images
kubectl rollout restart deploy/webapp-backend deploy/webapp-frontend -n chess
```

Frontend exposed on NodePort `30900` → `http://<vps-ip>:30900`.

## Local dev

```bash
# backend
cd serving/backend
pip install -r requirements.txt
uvicorn main:app --reload

# frontend (separate shell)
cd serving/frontend
npm install
BACKEND_URL=http://localhost:8000 npm run dev
```

## Endpoints

| Method | Path                              | Status     |
| ------ | --------------------------------- | ---------- |
| GET    | `/healthz`                        | done       |
| GET    | `/api/games/:id`                  | done       |
| POST   | `/api/eval`                       | done       |
| GET    | `/api/players/:name/profile`      | done       |
| POST   | `/api/coach`                      | phase 3    |
| GET    | `/api/exercise/:player`           | phase 4    |
| POST   | `/api/whatif`                     | phase 5    |
