FROM openjdk:8
RUN apt-get update && apt-get upgrade
RUN apt-get install -y maven
COPY krb5.conf /
COPY adaltas.keytab /
COPY . /project
RUN cd /project && mvn clean package
ENTRYPOINT ["java", "-jar", "/project/target/project-0.0.1.jar"]
