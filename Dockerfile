FROM centos:7.3.1611
RUN echo 'we are launching MQTT Bridge'
CMD mqtt_bridge
COPY ./mqtt_bridge /usr/local/bin/mqtt_bridge