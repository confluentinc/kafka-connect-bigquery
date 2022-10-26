FROM maven:3.8.6-openjdk-11-slim

ARG USER_ID
ARG GROUP_ID


RUN addgroup --gid $GROUP_ID jenkins
RUN adduser --disabled-login --uid $USER_ID --ingroup jenkins jenkins

RUN mkdir -p /var/maven/.m2
RUN chown jenkins:jenkins /var/maven/.m2 -R

ENV MAVEN_CONFIG "/var/maven/.m2"

RUN apt-get update && apt-get install -y git

USER jenkins