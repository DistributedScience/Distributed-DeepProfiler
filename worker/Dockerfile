#
#                                 - [ BROAD'16 ] -
#
# A docker instance for accessing AWS resources
# This wraps the cellprofiler docker registry
#


FROM michaelbornholdt/deep_profiler:jamboree

# Install S3FS 

RUN apt-get -y update           && \
    apt-get -y upgrade          && \
    apt-get -y install 		\
	automake 		\
	autotools-dev 		\
	g++ 			\
	git 			\
	libcurl4-gnutls-dev 	\
	libfuse-dev 		\
	libssl-dev 		\
	libxml2-dev 		\
	make pkg-config		\
	sysstat			\
	curl

WORKDIR /usr/local/src
RUN git clone https://github.com/s3fs-fuse/s3fs-fuse.git
WORKDIR /usr/local/src/s3fs-fuse
RUN ./autogen.sh
RUN ./configure
RUN make
RUN make install

# Install AWS CLI

RUN python3 -m pip install awscli 

# Install boto3

RUN python3 -m pip install boto3

# Install watchtower for logging

RUN python3 -m pip install watchtower

# Install pandas 

RUN python3 -m pip install pandas

# SETUP NEW ENTRYPOINT

RUN mkdir -p /home/ubuntu/
WORKDIR /home/ubuntu
COPY dp-worker.py .
COPY instance-monitor.py .
COPY run-worker.sh .
RUN chmod 755 run-worker.sh

WORKDIR /home/ubuntu
ENTRYPOINT ["./run-worker.sh"]
CMD [""]

