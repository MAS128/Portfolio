# About this project

![Visualisation](SCR-20250203-okjz.png)

The **AI-powered tagging application** is a *microservice-based* system designed to automate the *classification of social media posts* using a locally hosted large language model (LLM). It periodically downloads posts from an external PostgreSQL database and processes them through natural language understanding techniques to assign relevant tags. The tagging logic leverages both predefined tags and dynamically extracted hashtags, mentions, and named entities while ensuring accuracy by filtering out unwanted or banned tags.

Redis is utilized for caching and synchronization, while RabbitMQ facilitates message queuing for seamless task distribution. The application integrates with the rest of the system hosted in Microsoft Azure Cloud via VPN. Data ingestion and processing are handled through a Django-based backend, interfacing with the database using psycopg and executing SQL queries. The AI tagging logic is implemented using a self-hosted open-source LLM server (LmStudio), ensuring adaptability to various NLP models.

Additionally, the system incorporates a summarization module to generate concise insights from tagged content. Logging and monitoring are implemented via Python’s logging module to track system performance and error handling. The entire pipeline operates within a Docker containerized environment, ensuring easy deployment and maintainability across different infrastructure setups. The code was anonymized in accordance with my agreement with my previous employer.

---
