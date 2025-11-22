先给git配置一下代理，然后再往github推，为了不影响之后推送国内的代码所以推送完以后要把代理取消了

git config http.proxy http://127.0.0.1:10808
git config https.proxy http://127.0.0.1:10808


git config --unset http.proxy
git config --unset https.proxy

更新系统源
sudo apt update
安装 Git
sudo apt install -y git
验证安装
git --version

执行 uv_install.sh
执行 source ~/.bashrc 让 PATH 设置立刻生效
执行 cron.sh 配置定时任务