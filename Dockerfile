# Base image चुनें
FROM python:3.9-slim

# Working directory सेट करें
WORKDIR /usr/src/app

# dependencies इंस्टॉल करें
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# बाकी कोड कॉपी करें
COPY . .

# Webhook के लिए gunicorn का उपयोग करें (अगर main.py में 'app' ऑब्जेक्ट है)
CMD [ "gunicorn", "main:app", "-b", "0.0.0.0:8000" ]
