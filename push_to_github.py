#!/usr/bin/env python3
"""
创建GitHub仓库并推送代码
"""
import subprocess
import requests
import json
import sys

GITHUB_TOKEN = "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"
REPO_NAME = "evolving-agent"
REPO_DESCRIPTION = "Persistent Evolving Agent System - No idle state, always executing and optimizing"

def create_repo():
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    data = {
        "name": REPO_NAME,
        "description": REPO_DESCRIPTION,
        "private": True,
        "auto_init": False
    }
    
    try:
        response = requests.post(
            "https://api.github.com/user/repos",
            headers=headers,
            json=data
        )
        if response.status_code in [200, 201]:
            repo_info = response.json()
            print(f"✅ 私有仓库创建成功！")
            print(f"仓库地址: {repo_info['html_url']}")
            return repo_info['clone_url']
        else:
            print(f"⚠️  响应状态: {response.status_code}")
            print(f"响应内容: {response.text}")
            # 检查仓库是否已经存在
            check_url = f"https://api.github.com/repos/2358384171/{REPO_NAME}"
            check_response = requests.get(check_url, headers=headers)
            if check_response.status_code == 200:
                print("⚠️  仓库已存在，直接使用现有仓库")
                return check_response.json()['clone_url']
            else:
                print(f"❌ 无法创建或找到仓库")
                return None
    except Exception as e:
        print(f"❌ 创建仓库时出错: {e}")
        return None

def push_code(clone_url):
    # 添加远程仓库
    subprocess.run([
        "git", "remote", "add", "origin",
        clone_url.replace("https://", f"https://{GITHUB_TOKEN}@")
    ], cwd="/workspace/evolving_agent", check=False)
    
    # 推送到 main 分支
    print("📤 正在推送代码...")
    result = subprocess.run([
        "git", "push", "-u", "origin", "main"
    ], cwd="/workspace/evolving_agent", capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ 代码成功推送到 GitHub！")
    else:
        print("❌ 推送失败，尝试强制推送...")
        result2 = subprocess.run([
            "git", "push", "-u", "-f", "origin", "main"
        ], cwd="/workspace/evolving_agent", capture_output=True, text=True)
        if result2.returncode == 0:
            print("✅ 代码成功推送到 GitHub！")
        else:
            print(result2.stderr)

if __name__ == "__main__":
    print("="*60)
    print("创建 GitHub 私有仓库并推送代码")
    print("="*60)
    print()
    
    clone_url = create_repo()
    if clone_url:
        print()
        push_code(clone_url)
