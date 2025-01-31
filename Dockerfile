FROM --platform=linux/amd64 python:3.11

WORKDIR /app

# Copy dependency files first for better caching
COPY requirements.txt .
COPY pyproject.toml .

# Install dependencies
RUN pip install -r requirements.txt

# Copy the entire project structure
# We're copying everything to maintain the package hierarchy
COPY . .

# Install your package in development mode
# This makes the package importable from anywhere
RUN pip install -e .

# The -e flag (editable mode) creates a link to your source code
# This means Python will always use the latest version of your code
# and maintains the proper package structure

ENTRYPOINT ["python", "run.py"]