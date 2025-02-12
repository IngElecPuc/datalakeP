FROM alpine:3.21.2
WORKDIR /app/
COPY requirements.txt .
RUN apk add --no-cache python3 py3-pip
RUN python3 -m venv venv \
    && source venv/bin/activate \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && apk update \
    && apk add nano
COPY . .
ENV STAGE=Test
EXPOSE 5439

RUN addgroup appadmin && adduser -S -G appadmin appadmin
USER appadmin

CMD ["sh", "-c", ". venv/bin/activate && exec python3 src/main.py"]