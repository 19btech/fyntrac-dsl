# Fyntrac DSL Studio

This repository contains the DSL Studio frontend, the FastAPI backend, DSL function runtime support, and optional export artifacts used for model handoff.

## Main Runtime

- Frontend: `frontend/`
- Backend: `backend/`
- Startup script: `startup.sh`
- Stop script: `stop.sh`

The main application path used in this repo is:

```bash
./startup.sh
```

That starts the React frontend and the FastAPI backend exposed through `backend.server`.

## Optional Package

`FyntracPythonModel/` is a separate package for running exported/generated models outside the main backend service. It is not the runtime started by `startup.sh`.

## Development

Backend:

```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m backend.server
```

Frontend:

```bash
cd frontend
npm install
npm start
```

## Notes

- The canonical DSL runtime implementation lives under `backend/`.
- `memory/` and `ai/` contain internal reference material and notes.
- `frontend/build/` is generated output and can be recreated with `npm run build`.

## DSL Helper Example

`days_to_next(current_date, next_date, default=0)` returns the signed day difference between two dates and falls back to `default` when `next_date` is missing or invalid.
