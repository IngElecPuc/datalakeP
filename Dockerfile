FROM python:3.13.1-alpine3.21
RUN addgroup appadmin && adduser -S -G appadmin appadmin
USER appadmin
WORKDIR /app/
COPY --chown=appadmin requirements.txt .
#RUN pip install -r requirements.txt
COPY --chown=appadmin . .
ENV STAGE=Develop
EXPOSE 5439
