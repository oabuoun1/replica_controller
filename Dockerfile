FROM python:3
ADD replica_controller.py /
ADD tasks /tasks
RUN mkdir /jobs
ENTRYPOINT ["python3", "./replica_controller.py"]
CMD []