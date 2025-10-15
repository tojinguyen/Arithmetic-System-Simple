FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y git openssh-client build-essential && rm -rf /var/lib/apt/lists/*

RUN mkdir -p -m 0700 /root/.ssh && ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN --mount=type=ssh pip install "git+ssh://git@github.com/Cinnamon/mini.git@1.0.34"

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

