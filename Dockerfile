# 使用官方Python运行时作为父镜像
FROM python:3.7-slim

# 将工作目录设置为/app
WORKDIR /app

# 将当前目录的内容复制到容器的/app目录中
ADD . /app

# 安装在requirements.txt中列出的任何需要的包
RUN pip install --no-cache-dir -r requirements.txt

# 使端口80可供此容器外的环境使用
EXPOSE 80

# 定义环境变量
ENV NAME World

# 在容器启动时运行app.py
CMD ["python", "app.py"]