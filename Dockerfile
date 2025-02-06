FROM alpine:3.21.2
RUN addgroup appadmin && adduser -S -G appadmin appadmin
RUN apk add --no-cache python3 py3-pip
USER appadmin
WORKDIR /app/
COPY --chown=appadmin requirements.txt .
RUN pip install -r requirements.txt
COPY --chown=appadmin . .
ENV STAGE=Develop
EXPOSE 5439
