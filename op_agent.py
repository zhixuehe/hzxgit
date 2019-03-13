import op_mongo
import time
import datetime
import threading
import op_facebook_api
import pytz
import op_log


class Agent:
    def __init__(self, ad_set_id, min_roas, max_roas, init_bid_amount):
        self.ad_set_id = ad_set_id
        self.min_roas = min_roas
        self.max_roas = max_roas
        self.init_state = op_mongo.get_profile(self.ad_set_id)
        self.init_state_morning = op_mongo.get_profile(self.ad_set_id)
        self.init_bid_amount = init_bid_amount
        self.bid_amount = 0
        self.roas_0 = 0
        self.cold_clocks = 10
        self.count_n = 0
        self.temporary_closure = False
        self.profile_list = []
        self.hour_budget = 0.5     # 初始的hour_budegt
        self.begin = True
        self.max_bid_amount = 18000
        self.logger = op_log.init_log(self.ad_set_id)

    def start(self):
        # 开启线程控制agent状态和花费速率
        threading.Thread(target=self.spend_control).start()

    def spend_control(self):
        # 5分钟进行检查一次,出价调整最快10分钟一次
        # 如果roas大于一般情况的最大值，则视为表现好，故提出价，没有上限次数，只要好就提价，
        # 如果roas小于一般情况的最小值，则视为表现差，故降出价，
        # 如果roas处于设定区间内，则视为表现一般，则考虑花费控制，引入控制系数e
        # 在表现一般和表现差的时候，时段预算会时刻作用着，当时段实际花费超了，则关闭adset，这种关闭，会在下个时段重新气启动
        tz = pytz.timezone('Asia/Shanghai')
        time1 = datetime.datetime.now(tz).date()
        # 初始化当天出价
        res = op_facebook_api.set_bid_amount(self.ad_set_id, self.init_bid_amount)
        if res:
            print(self.ad_set_id, '凌晨 初始出价，初始出价成功，现在的出价为: ' + str(self.init_bid_amount))
            self.logger.info('凌晨 初始出价，初始出价成功，现在的出价为: ' + str(self.init_bid_amount))
        else:
            print(self.ad_set_id, '凌晨 初始出价，初始出价失败')
            self.logger.info('凌晨 初始出价，初始出价失败')

        while True:
            time_now = datetime.datetime.now(tz).date()
            if (time_now - time1).days == 1:
                break
            # 记录每一次检查的profile的值，用列表存放需要考虑的时段的profile值
            # 当roas为0时，我们视它为min_roas，若超过72次则冰封！
            today_profile = self.get_today_profile()
            self.profile_list.append(today_profile)
            if len(self.profile_list) > 73:  # 首次的时间是0分钟，调整
                del self.profile_list[0]

            if self.profile_list[-1]['spend'] - self.profile_list[0]['spend']:
                six_hours_roas = (self.profile_list[-1]['value'] - self.profile_list[0]['value']) / (self.profile_list[-1]['spend'] - self.profile_list[0]['spend'])
                if six_hours_roas != 0:
                    self.begin = False
            else:
                six_hours_roas = 0

            if six_hours_roas == 0 and self.begin == True:
                self.roas_0 += 1
                if self.roas_0 == 96:   # 也就是8小时都没付费，可调整，需讨论
                    res = op_facebook_api.set_adset(self.ad_set_id, 'PAUSED')  # 关闭adset 并且处于冰封状态
                    if res:
                        self.logger.info('因为此adset从凌晨开始 超过8小时没有付费，故将其关闭，今日不再启动，关闭成功')
                    else:
                        print(self.ad_set_id, '因为此adset从凌晨开始 超过8小时没有付费，故将其关闭，今日不再启动，但关闭失败')
                        self.logger.info('因为此adset从凌晨开始 超过8小时没有付费，故将其关闭，今日不再启动，但关闭失败')
                    # op_mongo.update_status_adset(self.ad_set_id)  # 将冰封的adset在mongo的集合中进行状态更新
                    # print(self.ad_set_id, '因为此adset从凌晨开始 超过8小时没有付费，故将其标记为【冰封】状态')
                    # self.logger.info('因为此adset从凌晨开始 超过8小时没有付费，故将其标记为【冰封】状态')
                    break
                six_hours_roas = self.min_roas

            # 计算控制系数e
            budget_dict = op_mongo.get_hour_budget(self.ad_set_id)
            if budget_dict:
                self.hour_budget = budget_dict['tmp_budget']
                self.init_state = op_mongo.get_profile(self.ad_set_id)
                res = op_facebook_api.set_adset(self.ad_set_id, 'ACTIVE')
                if res:
                    self.logger.info('因为此adset的在前一时段可能因为花费花完了，导致关闭，故新的时段将其开启，开启成功')
                else:
                    print(self.ad_set_id, '因为此adset的在前一时段可能因为花费花完了，导致关闭，故新的时段将其开启，但开启失败')
                    self.logger.info('因为此adset的在前一时段可能因为花费花完了，导致关闭，故新的时段将其开启，但开启失败')

            profile = self.get_profile()
            current_lifetime_spend = profile['spend']
            current_lifetime_hours = profile['lifetime'] / 60
            expect_spend = self.hour_budget * current_lifetime_hours
            if self.hour_budget == 0:
                print(self.ad_set_id, 'hour_budget is 0，出错，查看原因')
                self.logger.info('hour_budget is 0，出错，查看原因')
            if expect_spend == 0:
                e = 1.0
            else:
                e = round(current_lifetime_spend / expect_spend, 2)

            change_bid = False
            # 表现好
            if six_hours_roas > self.max_roas:
                if self.temporary_closure:
                    res = op_facebook_api.set_adset(self.ad_set_id, 'ACTIVE')
                    if res:
                        self.logger.info('此adset的时段花费花完后，发现其效果很棒，故将其从新开启，开启成功')
                        self.temporary_closure = False
                    else:
                        print(self.ad_set_id, '此adset的时段花费花完后，发现其效果很棒，故将其从新开启， 但开启失败')
                        self.logger.info('此adset的时段花费花完后，发现其效果很棒，故将其从新开启，但开启失败')

                if self.cold_clocks >= 10:
                    self.cold_clocks = 0
                    self.count_n = 0
                    self.logger.info('表现好，提')
                    change_bid = True
                    bid_ratio = 1.1
            # 表现差
            elif current_lifetime_spend >= self.hour_budget:
                # 触发关，时段临时
                if not self.temporary_closure:
                    res = op_facebook_api.set_adset(self.ad_set_id, 'PAUSED')  # 关闭adset，因为时段效果不好
                    if res:
                        self.logger.info('因为此adset的时段花费花完了，故将其此时段关闭，关闭成功')
                    else:
                        print(self.ad_set_id, '因为此adset的时段花费花完了，故将其在此时段关闭，但关闭失败')
                        self.logger.info('因为此adset的时段花费花完了，故将其此时段关闭，但关闭失败')

            elif six_hours_roas < self.min_roas:
                if self.cold_clocks >= 10:
                    self.cold_clocks = 0
                    self.count_n += 1
                    if self.count_n > 10:
                        # 触发关，且是冰封！
                        res = op_facebook_api.set_adset(self.ad_set_id, 'PAUSED')  # 关闭adset 并且处于冰封状态
                        if res:
                            self.logger.info('因为此adset表现很差，故将其关闭，今日不再启动，关闭成功')
                        else:
                            print(self.ad_set_id, '因为此adset表现很差，故将其关闭，今日不再启动，但关闭失败')
                            self.logger.info('因为此adset表现很差，故将其关闭，今日不再启动，但关闭失败')
                        # op_mongo.update_status_adset(self.ad_set_id)  # 将冰封的adset在mongo的集合中进行状态更新
                        # print(self.ad_set_id, '因为此adset表现很差，故将其标记为【冰封】状态')
                        # self.logger.info('因为此adset表现很差，故将其标记为【冰封】状态')
                        break
                    else:
                        self.logger.info('表现差，降')
                        change_bid = True
                        bid_ratio = 0.85
                        # 表现一般
            elif six_hours_roas >= self.min_roas and six_hours_roas <= self.max_roas:
                if self.cold_clocks >= 10:
                    self.cold_clocks = 0
                    if self.count_n > 0:
                        self.count_n -= 1
                    else:
                        self.count_n = 0

                    if e > 1.1:
                        self.logger.info('表现一般，还用钱快，降')
                        change_bid = True
                        bid_ratio = 0.9
                    elif e < 0.9:
                        self.logger.info('表现一般，且用不出钱，提')
                        change_bid = True
                        bid_ratio = 1.05


            if change_bid:
                if self.bid_amount == 0:
                    self.bid_amount = op_facebook_api.get_bid_amount(self.ad_set_id)
                if self.bid_amount > 0:
                    self.bid_amount = self.bid_amount * bid_ratio
                if self.bid_amount >= self.max_bid_amount:
                    self.bid_amount = self.max_bid_amount
                self.bid_amount = int(self.bid_amount)          # 出价必须为整数！
                res = op_facebook_api.set_bid_amount(self.ad_set_id, self.bid_amount)
                if res:
                    self.logger.info('调整出价成功，此时的出价为: ' + str(self.bid_amount))
                else:
                    print(self.ad_set_id, '调整出价失败，故将出价还原')
                    self.logger.info('调整出价失败，故将出价还原')
                    self.bid_amount /= bid_ratio

            self.cold_clocks += 5
            time.sleep(300)

    def get_profile(self):
        profile = op_mongo.get_profile(self.ad_set_id)
        for key in profile:
            profile[key] = round(profile[key] - self.init_state[key], 2)
        return profile

    def get_today_profile(self):
        profile = op_mongo.get_profile(self.ad_set_id)
        for key in profile:
            profile[key] = round(profile[key] - self.init_state[key], 2)
        return profile
