FROM alpine:3.21.2
#RUN addgroup appadmin && adduser -S -G appadmin appadmin
#USER appadmin
WORKDIR /app/
COPY requirements.txt .
RUN apk add --no-cache python3 py3-pip
RUN python3 -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
COPY . .
ENV STAGE=Develop
EXPOSE 5439
