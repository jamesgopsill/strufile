FROM amazonlinux:2023

# Needed to unpack the vscode extension
RUN yum install -y tar gzip

# Development tools used by rust
RUN yum groupinstall -y "Development Tools"

# Install the rust language
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y