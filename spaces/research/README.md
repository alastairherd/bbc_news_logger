---
title: BBC News Archive Research
sdk: gradio
app_file: app.py
pinned: false
license: mit
---

# BBC News archive research

This non-Docker, free-CPU Space provides bounded cited synthesis for the BBC News Surface Lab.
The public dashboard performs BGE Small retrieval locally and sends at most ten BBC evidence rows
to this service. DeepSeek V4 Flash then answers only from that evidence.

Required Space secret: `DEEPSEEK_API_KEY`.
