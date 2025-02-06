FROM node:23.7.0-alpine3.20
RUN addgroup appadmin && adduser -S -G appadmin appadmin
USER appadmin
WORKDIR /app/
COPY --chown=appadmin requirements.txt .
RUN pip install -r requirements.txt
COPY --chown=appadmin . .
ENV STAGE=Develop
EXPOSE 5439
