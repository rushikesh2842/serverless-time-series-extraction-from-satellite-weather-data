# Start from AWS Lambda Python 3.11 base image
FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies (GCC >= 9.3, netCDF libraries, etc.)
RUN yum -y update \
    && yum -y install gcc gcc-c++ make cmake libcurl-devel hdf5-devel netcdf-devel tar gzip wget\
    && yum clean all

RUN yum remove -y cmake

RUN curl -LO https://github.com/Kitware/CMake/releases/download/v3.27.2/cmake-3.27.2-linux-x86_64.sh && \
    chmod +x cmake-3.27.2-linux-x86_64.sh && \
    ./cmake-3.27.2-linux-x86_64.sh --skip-license --prefix=/usr/local && \
    rm cmake-3.27.2-linux-x86_64.sh


# Install Python dependencies
COPY requirements.txt ./

RUN pip3 install --upgrade pip wheel setuptools
RUN pip3 install --prefer-binary --no-cache-dir -r requirements.txt

# Copy function code
COPY app/ ${LAMBDA_TASK_ROOT}

# Command for Lambda to run your handler
CMD ["lambda_function.lambda_handler"]