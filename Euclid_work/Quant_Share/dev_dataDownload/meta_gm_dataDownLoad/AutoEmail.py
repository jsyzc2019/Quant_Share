import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from datetime import datetime


def AutoEmail(title=None, content=None, sender='2379928684@qq.com', config_path='AutoEmail_config.txt'):
    # header setting
    if not os.path.exists(config_path):
        raise FileNotFoundError("should set para config_path, which contains pass & user")
    with open(config_path, 'rt', encoding='utf-8') as f:
        AutoEmail_config = [i.strip() for i in f.readlines()]
    my_pass = AutoEmail_config[0]  # 发件人邮箱授权码
    my_user = AutoEmail_config[1]  # 收件人邮箱账号
    my_sender = sender  # 填写发信人的邮箱账号

    msg = MIMEMultipart()
    # 主题 正文
    if not content:
        content = "this is auto content"  # 正文内容
    if not title:
        title = 'Auto Title for Test'
    msg.attach(MIMEText(content, 'plain', 'utf-8'))
    msg['Subject'] = "{}-{}".format(title, datetime.today().strftime("%Y-%m-%d %H:%M:%S"))  # 邮件主题

    # 抬头
    msg['From'] = formataddr(("Windows Remote", my_sender))  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
    msg['To'] = formataddr(("Euclid Local", my_user))  # 括号里的对应收件人邮箱昵称、收件人邮箱账号

    # # 添加附件
    # fileFullPath = r'E:\Euclid\Quant_Share\Euclid_work\Quant_Share\dev_files\fundamentals_income_info.xlsx'
    # att1 = MIMEText(open(fileFullPath, 'rb').read(), 'base64', 'utf-8')  # 打开附件
    # att1['Content-Type'] = 'application/octet-stream'  # 设置类型是流媒体格式
    # att1['Content-Disposition'] = 'attachment;filename={}'.format(os.path.split(fileFullPath)[-1])  # 设置描述信息
    # msg.attach(att1)

    server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器
    server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱授权码
    server.sendmail(my_sender, [my_user, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
    server.quit()  # 关闭连接
    return True


if __name__ == '__main__':
    if AutoEmail():
        print("邮件发送成功")
    else:
        print("邮件发送失败")
