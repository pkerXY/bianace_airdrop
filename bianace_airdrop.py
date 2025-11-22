# pip install serverchan-sdk
import requests
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from serverchan_sdk import sc_send
import os
import re
import time
import pytz
from config import (
    SERVERCHAN_KEY,
    DB_FILE,
    LOG_FILE,
    LOG_RETENTION_DAYS,
    HIGH_VALUE_THRESHOLD,
    MEDIUM_VALUE_THRESHOLD,
    REMINDER_3MIN,
    REMINDER_COUNT,
    REMINDER_INTERVAL,
    DATA_URL,
    PRICE_URL,
    HEADERS
)

# 设置北京时区
BEIJING_TZ = pytz.timezone('Asia/Shanghai')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# API配置（从 config.py 导入）


class AirdropMonitor:
    def __init__(self):
        self.conn = None
        self.init_database()
    
    def init_database(self):
        """初始化数据库"""
        self.conn = sqlite3.connect(DB_FILE)
        cursor = self.conn.cursor()
        
        # 创建空投记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS airdrops (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL,
                name TEXT,
                date TEXT NOT NULL,
                time TEXT,
                amount TEXT,
                points TEXT,
                price REAL,
                total_value REAL,
                phase INTEGER,
                type TEXT,
                status TEXT,
                contract_address TEXT,
                chain_id TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notified_new INTEGER DEFAULT 0,
                notified_3min INTEGER DEFAULT 0,
                UNIQUE(token, date, phase)
            )
        ''')
        
        # 创建状态变化记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS status_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                airdrop_id INTEGER,
                change_type TEXT,
                old_value TEXT,
                new_value TEXT,
                change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notified INTEGER DEFAULT 0,
                FOREIGN KEY (airdrop_id) REFERENCES airdrops (id)
            )
        ''')
        
        self.conn.commit()
        logging.info("数据库初始化完成")

    def fetch_api_data(self):
        """获取API数据"""
        try:
            # 获取空投数据
            response = requests.get(DATA_URL, headers=HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()

            # 获取价格数据
            price_response = requests.get(PRICE_URL, headers=HEADERS, timeout=30)
            price_response.raise_for_status()
            price_data = price_response.json()

            # 调试：打印价格数据结构
            logging.info(f"价格数据类型: {type(price_data)}")
            if isinstance(price_data, list):
                logging.info(f"价格数据是列表，长度: {len(price_data)}")
            elif isinstance(price_data, dict):
                logging.info(f"价格数据是字典，keys: {price_data.keys()}")

            # 修复：正确处理价格数据结构
            prices_dict = {}
            if isinstance(price_data, list):
                # 如果是列表，遍历每个项目
                for item in price_data:
                    if isinstance(item, dict):
                        # 假设每个项目都有 'token' 字段作为键
                        token_key = item.get('token') or item.get('symbol') or str(item.get('address', ''))
                        if token_key:
                            prices_dict[str(token_key)] = item
            elif isinstance(price_data, dict):
                # 如果是字典，检查是否有 'prices' 字段
                prices_dict = price_data.get('prices', {})
                # 如果没有 'prices' 字段，但字典包含价格信息，直接使用
                if not prices_dict and price_data:
                    prices_dict = price_data

            logging.info(f"成功获取API数据: {len(data.get('airdrops', []))} 个空投, 转换后 {len(prices_dict)} 个价格")

            return data.get('airdrops', []), prices_dict
        except Exception as e:
            logging.error(f"获取API数据失败: {str(e)}", exc_info=True)
            raise
    
    def calculate_value(self, amount, token, prices):
        """计算空投价值"""
        if not amount or not str(amount).replace('.', '').replace('-', '').isdigit():
            return None, None
        
        token_price_info = prices.get(str(token), {})
        if not token_price_info:
            return None, None
        
        price = token_price_info.get('price', 0)
        dex_price = token_price_info.get('dex_price', 0)
        final_price = price if price > 0 else dex_price
        
        if final_price <= 0:
            return None, None
        
        try:
            total_value = float(amount) * final_price
            return final_price, total_value
        except:
            return final_price, None
    
    def get_airdrop_by_key(self, token, date, phase):
        """根据唯一键获取空投记录"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM airdrops 
            WHERE token = ? AND date = ? AND phase = ?
        ''', (token, date, phase))
        return cursor.fetchone()
    
    def insert_new_airdrop(self, airdrop, price, total_value):
        """插入新空投记录"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO airdrops 
            (token, name, date, time, amount, points, price, total_value, phase, type, status, contract_address, chain_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            airdrop.get('token'),
            airdrop.get('name'),
            airdrop.get('date'),
            airdrop.get('time', ''),
            airdrop.get('amount', ''),
            airdrop.get('points', ''),
            price,
            total_value,
            airdrop.get('phase'),
            airdrop.get('type'),
            airdrop.get('status'),
            airdrop.get('contract_address'),
            airdrop.get('chain_id')
        ))
        self.conn.commit()
        return cursor.lastrowid
    
    def update_airdrop(self, airdrop_id, airdrop, price, total_value):
        """更新空投记录"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE airdrops 
            SET name = ?, time = ?, amount = ?, points = ?, price = ?, total_value = ?, 
                type = ?, status = ?, contract_address = ?, chain_id = ?, last_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            airdrop.get('name'),
            airdrop.get('time', ''),
            airdrop.get('amount', ''),
            airdrop.get('points', ''),
            price,
            total_value,
            airdrop.get('type'),
            airdrop.get('status'),
            airdrop.get('contract_address'),
            airdrop.get('chain_id'),
            airdrop_id
        ))
        self.conn.commit()
    
    def record_status_change(self, airdrop_id, change_type, old_value, new_value):
        """记录状态变化"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO status_changes (airdrop_id, change_type, old_value, new_value)
            VALUES (?, ?, ?, ?)
        ''', (airdrop_id, change_type, str(old_value), str(new_value)))
        self.conn.commit()
        return cursor.lastrowid
    
    def send_notification(self, title, content, tag="空投提醒", priority="normal"):
        """发送通知"""
        try:
            # 根据优先级添加emoji
            if priority == "high":
                title = f"🔴 {title}"
            elif priority == "medium":
                title = f"🟡 {title}"
            elif priority == "urgent":
                title = f"🚨 {title}"
            
            response = sc_send(SERVERCHAN_KEY, title, content, {"tags": tag})
            logging.info(f"通知已发送: {title}")
            return response
        except Exception as e:
            logging.error(f"发送通知失败: {str(e)}", exc_info=True)
            return None
    
    def format_airdrop_message(self, airdrop_data, show_title=True):
        """格式化空投消息"""
        token = airdrop_data.get('token', '未知')
        name = airdrop_data.get('name', '未知')
        date = airdrop_data.get('date', '未知')
        time = airdrop_data.get('time', '')
        amount = airdrop_data.get('amount', '')
        points = airdrop_data.get('points', '')
        price = airdrop_data.get('price')
        total_value = airdrop_data.get('total_value')
        airdrop_type = airdrop_data.get('type', '未知')
        phase = airdrop_data.get('phase', '未知')
        
        msg = ""
        if show_title:
            msg += f"### {name} ({token})\n\n"
        
        msg += f"- **日期**: {date}\n"
        
        if time:
            msg += f"- **时间**: {time}\n"
        else:
            msg += f"- **时间**: ⚠️ 时间未确定\n"
        
        msg += f"- **数量**: {amount if amount else '未知'}\n"
        msg += f"- **分数门槛**: {points if points else '无'}\n"
        
        if price and price > 0:
            msg += f"- **代币价格**: ${price:.6f}\n"
            if total_value and total_value > 0:
                msg += f"- **预估价值**: ${total_value:.2f}\n"
        else:
            msg += f"- **代币价格**: ⚠️ 目前无法计算价值\n"
        
        msg += f"- **类型**: {airdrop_type}\n"
        msg += f"- **阶段**: Phase {phase}\n"
        
        return msg
    
    def check_and_notify_new(self, airdrop, price, total_value):
        """检查并通知新空投"""
        token = airdrop.get('token')
        date = airdrop.get('date')
        phase = airdrop.get('phase')
        
        existing = self.get_airdrop_by_key(token, date, phase)
        
        if existing is None:
            # 新空投
            airdrop_id = self.insert_new_airdrop(airdrop, price, total_value)
            
            # 立即通知新空投
            airdrop_data = {**airdrop, 'price': price, 'total_value': total_value}
            priority = self.get_priority_by_value(total_value)
            
            title = f"新空投发现: {airdrop.get('name')}"
            content = self.format_airdrop_message(airdrop_data)
            self.send_notification(title, content, tag="新空投", priority=priority)
            
            # 标记已通知
            cursor = self.conn.cursor()
            cursor.execute('UPDATE airdrops SET notified_new = 1 WHERE id = ?', (airdrop_id,))
            self.conn.commit()
            
            logging.info(f"发现新空投: {token} - {airdrop.get('name')}, 价值等级: {priority}")
            return airdrop_id, True
        else:
            return existing[0], False
    
    def get_priority_by_value(self, total_value):
        """根据价值获取优先级"""
        if total_value is None:
            return "normal"
        if total_value >= HIGH_VALUE_THRESHOLD:
            return "high"
        elif total_value >= MEDIUM_VALUE_THRESHOLD:
            return "medium"
        else:
            return "normal"
    
    def check_status_changes(self, airdrop_id, old_data, new_airdrop, new_price, new_total_value):
        """检查状态变化"""
        changes = []
        
        # 检查时间变化 - 任何时候变化都要通知（最重要）
        old_time = old_data[4] if old_data[4] else ""
        new_time = new_airdrop.get('time', '')
        # 标准化比较
        old_time_normalized = str(old_time).strip()
        new_time_normalized = str(new_time).strip()
        if old_time_normalized != new_time_normalized and new_time_normalized:
            change_id = self.record_status_change(airdrop_id, 'time_updated', old_time, new_time)
            if old_time_normalized:
                # 时间发生了改变
                changes.append({
                    'type': 'time_updated',
                    'old': old_time,
                    'new': new_time,
                    'message': f"⚠️ 时间已变更: {old_time} → {new_time}"
                })
            else:
                # 时间从无到有
                changes.append({
                    'type': 'time_updated',
                    'old': old_time,
                    'new': new_time,
                    'message': f"时间已确定: {new_time}"
                })
        
        # 不再监控价格变化，避免过多通知
        
        # 检查数量变化 - 只在从无到有时通知
        old_amount = old_data[5] if old_data[5] else ""
        new_amount = new_airdrop.get('amount', '')
        old_amount_normalized = str(old_amount).strip()
        new_amount_normalized = str(new_amount).strip()
        # 只有从空到有值时才通知
        if not old_amount_normalized and new_amount_normalized:
            change_id = self.record_status_change(airdrop_id, 'amount_updated', old_amount, new_amount)
            changes.append({
                'type': 'amount_updated',
                'old': old_amount,
                'new': new_amount,
                'message': f"数量已确定: {new_amount}"
            })
        
        # 检查分数门槛变化 - 只在从无到有时通知
        old_points = old_data[6] if old_data[6] else ""
        new_points = new_airdrop.get('points', '')
        old_points_normalized = str(old_points).strip()
        new_points_normalized = str(new_points).strip()
        # 只有从空到有值时才通知
        if not old_points_normalized and new_points_normalized:
            change_id = self.record_status_change(airdrop_id, 'points_updated', old_points, new_points)
            changes.append({
                'type': 'points_updated',
                'old': old_points,
                'new': new_points,
                'message': f"分数门槛已确定: {new_points}"
            })
        
        # 检查价值变化 - 只在从无到有时通知
        old_total_value = old_data[8]
        if old_total_value is None and new_total_value is not None and new_total_value > 0:
            change_id = self.record_status_change(airdrop_id, 'value_updated', 'None', new_total_value)
            changes.append({
                'type': 'value_updated',
                'old': None,
                'new': new_total_value,
                'message': f"预估价值已确定: ${new_total_value:.2f}"
            })
        
        return changes
    
    def notify_status_changes(self, airdrop, changes, price, total_value):
        """通知状态变化"""
        if not changes:
            return
        
        title = f"状态更新: {airdrop.get('name')}"
        content = f"### {airdrop.get('name')} ({airdrop.get('token')})\n\n"
        content += "**变化内容:**\n\n"
        
        for change in changes:
            content += f"- {change['message']}\n"
        
        content += f"\n---\n\n**当前信息:**\n\n"
        airdrop_data = {**airdrop, 'price': price, 'total_value': total_value}
        content += self.format_airdrop_message(airdrop_data, show_title=False)
        
        priority = self.get_priority_by_value(total_value)
        self.send_notification(title, content, tag="状态变化", priority=priority)
        
        logging.info(f"状态变化通知: {airdrop.get('name')}, 变化数: {len(changes)}")
    
    def check_upcoming_airdrops(self):
        """检查即将开始的空投（10分钟内）"""
        now = self.get_beijing_time()
        cursor = self.conn.cursor()
        
        # 查找今天有时间且未提醒的空投
        cursor.execute('''
            SELECT * FROM airdrops 
            WHERE date = ? AND time != '' AND time IS NOT NULL AND notified_3min = 0
        ''', (now.strftime('%Y-%m-%d'),))
        
        airdrops = cursor.fetchall()
        upcoming_airdrops = []
        
        for airdrop in airdrops:
            date_str = airdrop[3]
            time_str = airdrop[4]
            
            try:
                # 解析空投时间（使用北京时区）
                airdrop_datetime_naive = datetime.strptime(f"{date_str} {time_str}", '%Y-%m-%d %H:%M')
                airdrop_datetime = BEIJING_TZ.localize(airdrop_datetime_naive)
                time_diff_minutes = (airdrop_datetime - now).total_seconds() / 60
                
                # 如果在10分钟内开始
                if 0 < time_diff_minutes <= 10:
                    upcoming_airdrops.append({
                        'airdrop': airdrop,
                        'datetime': airdrop_datetime,
                        'time_diff_minutes': time_diff_minutes
                    })
                    logging.info(f"发现即将开始的空投: {airdrop[2]}, 剩余 {time_diff_minutes:.1f} 分钟")
            except Exception as e:
                logging.error(f"解析时间失败: {date_str} {time_str}, 错误: {str(e)}")
        
        return upcoming_airdrops
    
    def wait_and_send_reminders(self, airdrop_info):
        """等待并发送连续提醒"""
        airdrop = airdrop_info['airdrop']
        airdrop_datetime = airdrop_info['datetime']
        
        airdrop_id = airdrop[0]
        airdrop_data = {
            'token': airdrop[1],
            'name': airdrop[2],
            'date': airdrop[3],
            'time': airdrop[4],
            'amount': airdrop[5],
            'points': airdrop[6],
            'price': airdrop[7],
            'total_value': airdrop[8],
            'phase': airdrop[9],
            'type': airdrop[10]
        }
        
        # 计算需要等待的时间（等到开始前3分钟）
        reminder_time = airdrop_datetime - timedelta(minutes=REMINDER_3MIN)
        now = self.get_beijing_time()
        wait_seconds = (reminder_time - now).total_seconds()
        
        if wait_seconds > 0:
            logging.info(f"等待 {wait_seconds:.0f} 秒后发送提醒: {airdrop[2]}")
            time.sleep(wait_seconds)
        
        # 连续发送3条提醒
        for i in range(REMINDER_COUNT):
            now = self.get_beijing_time()
            remaining_minutes = (airdrop_datetime - now).total_seconds() / 60
            
            title = f"🚨 空投提醒 ({i+1}/{REMINDER_COUNT}): {airdrop[2]}"
            content = f"## ⏰ 空投即将在 {remaining_minutes:.1f} 分钟后开始！\n\n"
            content += f"**这是第 {i+1} 次提醒（共 {REMINDER_COUNT} 次）**\n\n"
            content += self.format_airdrop_message(airdrop_data)
            
            self.send_notification(title, content, tag="紧急提醒", priority="urgent")
            logging.info(f"已发送第 {i+1}/{REMINDER_COUNT} 次提醒: {airdrop[2]}")
            
            # 如果不是最后一次，等待间隔时间
            if i < REMINDER_COUNT - 1:
                time.sleep(REMINDER_INTERVAL)
        
        # 标记已提醒
        cursor = self.conn.cursor()
        cursor.execute('UPDATE airdrops SET notified_3min = 1 WHERE id = ?', (airdrop_id,))
        self.conn.commit()
        logging.info(f"已完成所有提醒并标记: {airdrop[2]}")
    
    def get_beijing_time(self):
        """获取北京时间"""
        return datetime.now(BEIJING_TZ)

    def is_airdrop_expired(self, airdrop):
        """检查空投是否已过期"""
        date_str = airdrop.get('date')
        time_str = airdrop.get('time', '')

        if not date_str:
            return False

        try:
            now = self.get_beijing_time()

            # 首先检查日期格式是否正确
            try:
                airdrop_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                logging.warning(f"日期格式错误: {date_str}")
                return False

            # 如果有具体时间且是有效的时间格式（HH:MM）
            if time_str and time_str.strip():
                # 检查是否是有效的时间格式，过滤掉 "Delay" 等非时间字符串
                if re.match(r'^\d{1,2}:\d{2}$', time_str.strip()):
                    try:
                        airdrop_datetime_naive = datetime.strptime(f"{date_str} {time_str}", '%Y-%m-%d %H:%M')
                        airdrop_datetime = BEIJING_TZ.localize(airdrop_datetime_naive)
                        # 如果当前时间已经超过空投时间，则认为已过期
                        if now > airdrop_datetime:
                            return True
                    except ValueError:
                        logging.warning(f"时间解析失败: {date_str} {time_str}")
                        # 时间格式无效，只比较日期
                        if now.date() > airdrop_date:
                            return True
                else:
                    # 不是有效时间格式，只比较日期
                    logging.debug(f"跳过无效时间格式: {time_str}")
                    if now.date() > airdrop_date:
                        return True
            else:
                # 如果没有具体时间，只比较日期
                if now.date() > airdrop_date:
                    return True

            return False
        except Exception as e:
            logging.error(f"检查过期时间失败: {date_str} {time_str}, 错误: {str(e)}")
            return False
    
    def process_airdrops(self):
        """处理所有空投"""
        try:
            airdrops, prices = self.fetch_api_data()
            
            now = self.get_beijing_time()
            today = now.strftime('%Y-%m-%d')
            today_airdrops = [a for a in airdrops if a.get('date') == today]
            
            logging.info(f"今天共有 {len(today_airdrops)} 个空投")
            
            active_count = 0
            expired_count = 0
            
            for airdrop in today_airdrops:
                # 检查空投是否已过期
                if self.is_airdrop_expired(airdrop):
                    expired_count += 1
                    logging.info(f"跳过已过期空投: {airdrop.get('name')} - {airdrop.get('time')}")
                    continue
                
                active_count += 1
                token = airdrop.get('token')
                date = airdrop.get('date')
                phase = airdrop.get('phase')
                amount = airdrop.get('amount')
                
                # 计算价值
                price, total_value = self.calculate_value(amount, token, prices)
                
                # 检查是否为新空投
                airdrop_id, is_new = self.check_and_notify_new(airdrop, price, total_value)
                
                # 如果不是新空投，检查状态变化
                if not is_new:
                    old_data = self.get_airdrop_by_key(token, date, phase)
                    changes = self.check_status_changes(airdrop_id, old_data, airdrop, price, total_value)
                    
                    # 更新数据库
                    self.update_airdrop(airdrop_id, airdrop, price, total_value)
                    
                    # 如果有变化，发送通知
                    if changes:
                        self.notify_status_changes(airdrop, changes, price, total_value)
            
            # 检查即将开始的空投（10分钟内）
            upcoming_airdrops = self.check_upcoming_airdrops()
            
            logging.info(f"本轮监控完成 - 活跃: {active_count}, 已过期: {expired_count}, 即将开始: {len(upcoming_airdrops)}")
            
            # 如果有即将开始的空投，等待并发送连续提醒
            if upcoming_airdrops:
                logging.info(f"发现 {len(upcoming_airdrops)} 个即将开始的空投，进入等待提醒模式")
                for upcoming in upcoming_airdrops:
                    self.wait_and_send_reminders(upcoming)
            
        except Exception as e:
            logging.error(f"处理空投时出错: {str(e)}", exc_info=True)
            self.send_notification(
                "监控程序错误", 
                f"处理空投数据时发生错误:\n\n{str(e)}", 
                tag="系统错误",
                priority="high"
            )
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


def main():
    """主函数"""
    monitor = AirdropMonitor()
    try:
        monitor.process_airdrops()
    except Exception as e:
        logging.error(f"程序执行失败: {str(e)}", exc_info=True)
    finally:
        monitor.close()


if __name__ == "__main__":
    main()