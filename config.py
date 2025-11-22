# 空投监控配置文件

# Server酱配置
SERVERCHAN_KEY = "sctp12664t2e5bt8djdrnfbvy4qjvwfn"

# 数据库配置
DB_FILE = "airdrop_monitor.db"

# 日志配置
LOG_FILE = "airdrop_monitor.log"
LOG_RETENTION_DAYS = 7  # 日志保留天数

# 价值分级阈值（美元）
HIGH_VALUE_THRESHOLD = 50  # 高价值空投阈值
MEDIUM_VALUE_THRESHOLD = 10  # 中价值空投阈值

# 提醒时间配置（分钟）
REMINDER_3MIN = 3  # 开始前3分钟提醒
REMINDER_COUNT = 3  # 连续提醒次数
REMINDER_INTERVAL = 1  # 提醒间隔（秒）

# API配置
DATA_URL = 'https://alpha123.uk/api/data?fresh=1'
PRICE_URL = 'https://alpha123.uk/api/price/?batch=today'

# Cookie配置（需要定期更新）
COOKIE = '_clck=wocq1w%5E2%5Eg0j%5E0%5E2119; cf_clearance=UJwZBV.ojw7Ar2prI72KtQ6EtXA6W5mP176g_VLK4Jw-1761620047-1.2.1.1-qY3y4Ef9nwutk_KwSSJJfxzEmFnBub25xIwB61DmeknKaPAf532qiZTJs3GwjqEyaSb630T_nSYn1Wads4EATGwdE_69peOLBHsfLk0fChu94TsBg9hDihnWH02.gx4sNqmU0SF5jFnzgbNZWDc7mAFoJUwAcLdDraFMb84EgVCFbJFE4_kocFgakTAhTMP1ErWTI2Q0lgmKCYPMzJShZPKzCJiqF3mF.y5HXWtBFQ8; _clsk=1tdhnrt%5E1761638248686%5E3%5E0%5Ee.clarity.ms%2Fcollect'

# 请求Headers
HEADERS = {
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9,en-CN;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'cookie': COOKIE,
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://alpha123.uk/zh/',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
}
