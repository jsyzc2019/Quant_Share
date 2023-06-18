
import os
import argparse
parser = argparse.ArgumentParser(description="运行网页端量化的附属指令")
parser.add_argument("-m", "--model_name", type=str, default='main.py', help="模型路径")
parser.add_argument("-p", "--port", type=int, default=8787)
args = parser.parse_args()
model_path = os.path.join(os.path.dirname(__file__),args.model_name)
os.system(f"python -m streamlit run {model_path} --server.port {args.port}")