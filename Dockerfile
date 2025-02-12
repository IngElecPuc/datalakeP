FROM alpine:3.21.2
#RUN addgroup appadmin && adduser -S -G appadmin appadmin
#USER appadmin
WORKDIR /app/
COPY requirements.txt .
RUN apk add --no-cache python3 py3-pip
RUN python3 -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
RUN apk update && apd add nano
COPY . .
ENV STAGE=Develop
ENV PYTHONPATH=/app/src:$PYTHONPATH
EXPOSE 5439
