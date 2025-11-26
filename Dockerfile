# ===========================================================
# 1. BUILDER STAGE
# - Installe Python + outils de build
# - Installe toutes les dépendances
# - Exécute pytest (fail → stop)
# ===========================================================

FROM python:3.13 AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Libraries système (pour compilations éventuelles)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copier uniquement les fichiers nécessaires pour installer les deps
COPY requirements.txt /app/
COPY pyproject.toml /app/

# Installer dépendances (cachées dans cette stage)
RUN pip install --no-cache-dir -r requirements.txt

# Copier TOUT le code dans le builder
COPY . /app

# -----------------------------
# EXÉCUTER LES TESTS
# -----------------------------
# Si pytest retourne une erreur → le build Docker s'arrête ici
RUN pytest -q --disable-warnings --maxfail=1


# ===========================================================
# 2. RUNTIME STAGE
# - Image finale minimaliste
# - Copie seulement le strict nécessaire depuis builder
# ===========================================================

FROM python:3.13 AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copier uniquement les libs Python installées dans builder
COPY --from=builder /usr/local/lib/python3.13/ /usr/local/lib/python3.13/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copier uniquement ton code source
COPY --from=builder /app /app

# ENTRYPOINT pour Step Functions + ECS Fargate
ENTRYPOINT ["python", "main.py"]
