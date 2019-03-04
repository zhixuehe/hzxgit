import mongo
import time
import datetime
import threading
import facebook_api
import pytz
import log

class Agent:
    def __init__(self, ad_set_id, min_budget, min_hours, max_cpi):
        self.ad_set_id = ad_set_id
        self.init_state = mongo.get_profile(self.ad_set_id)
        self.init_state_morning = mongo.get_profile(self.ad_set_id)
        self.init_state_status = mongo.get_profile(self.ad_set_id)
        self.min_budget = min_budget
        self.min_hours = min_hours
        self.max_cpi = max_cpi
        self.current_budget = min_budget
        self.alpha = 0.005
        self.phase = 'impression'
        self.impression_count = int(self.min_budget/(self.alpha*self.max_cpi))
        self.current_max_hours = min_hours
        self.install_count = int(2*self.min_budget/self.max_cpi)
        self.pay_times_count = 1
        self.pay_users_count = 2
        self.is_cold = True
        self.cold_clocks = 0
        self.adjust_direction = 0
        self.last_e_value = 0
        self.bid_amount = 0
        self.init_bid_amount = 10000
        self.logger = log.init_log(self.ad_set_id)


    def start(self):
        # 开启线程控制agent状态和花费速率
        threading.Thread(target=self.check_run).start()


    def spend_control(self):
        # 每5分钟，通过控制系数来调整出价
        # 完成一次调价后，默认有半小时（30分钟）的冷却时间（如果在出价调整后e一直上升或下降，则是正常的）
        # 如果调价后，e出现向相关的方向变化的情况，则冷却时间减少5分钟
        # 当从最近一次调整的时刻开始，到达冷却时间后，允许调价
        # 获取数据
        status_profile = self.get_status_profile()
        current_status_spend = status_profile['spend']
        current_status_hours = status_profile['lifetime'] / 60
        # 计算控制系数e
        expect_spend = self.current_budget * current_status_hours * 1.0 / self.current_max_hours
        if expect_spend == 0:
            return
        e = round(current_status_spend / expect_spend, 2)
        # 如果在冷却状态，则可以根据条件调整出价
        if self.is_cold:
            # 当e大于1.1时，根据与1的间距大小下调出价
            change_bid = False
            bid_ratio = 1
            if e > 1.1:
                self.logger.info('花费速度较快，进行降价处理')
                self.adjust_direction = -1
                change_bid = True
                bid_ratio = 0.95
            # 当e小于0.9时，根据与1的间距大小上调出价
            elif e < 0.9:
                self.logger.info('花费速度较慢，进行提价处理')
                self.adjust_direction = 1
                change_bid = True
                bid_ratio = 1.05
            if change_bid:
                self.is_cold = False
                if self.bid_amount == 0:
                    self.bid_amount = facebook_api.get_bid_amount(self.ad_set_id)
                if self.bid_amount > 0:
                    self.bid_amount = int(self.bid_amount * bid_ratio)    # bid_amount是整数
                    if self.bid_amount > 18000:
                        self.bid_amount = 18000
                    out_status = facebook_api.set_bid_amount(self.ad_set_id, self.bid_amount)
                    if out_status:
                        self.logger.info('调整出价成功，下次调价至少要15分钟以上，new_bid_amount为: ' + str(self.bid_amount))
                    else:
                        print(self.ad_set_id, '调整出价失败，将出价还原， 下次调价至少要15分钟以上')
                        self.logger.info('调整出价失败，将出价还原， 下次调价至少要15分钟以上')
                        self.bid_amount = int(self.bid_amount / bid_ratio)
                self.last_e_value = e
                self.cold_clocks = 30
        else:
            self.cold_clocks = self.cold_clocks - 5
            if self.cold_clocks <= 0:
                self.is_cold = True
            else:
                if self.adjust_direction == -1 and e > self.last_e_value:
                    self.cold_clocks = self.cold_clocks - 5
                if self.adjust_direction == 1 and e < self.last_e_value:
                    self.cold_clocks = self.cold_clocks - 5


    def check_run(self):
        # 若日期发生改变，需要对预算进行调整，具体是按不同status进行余额等同当日初始预算。
        # 若满足重组要求，则进行重组，重组结束对出价和预算继续重新设定
        # 对targrting按好、坏进行分析并存表
        tz = pytz.timezone('Asia/Shanghai')
        time1 = datetime.datetime.now(tz).date()
        while True:
            time_now = datetime.datetime.now(tz).date()
            if (time_now - time1).days == 1:
                # 如果一个所处状态跨天了，则需要对它进行预算的重新分配
                # daily_budget的分配标准是依据此adset在所处状态下应该的预算 - 该adset的current_lifetime_spend
                time1 = time_now
                self.init_state_morning = mongo.get_profile(self.ad_set_id)
                profile = self.get_profile()
                current_lifetime_spend = profile['spend']
                if self.phase == 'install':
                    daily_budget = self.min_budget*200 - current_lifetime_spend * 100
                elif self.phase == 'pay_times':
                    daily_budget = self.min_budget*300 - current_lifetime_spend * 100
                elif self.phase == 'pay_users':
                    daily_budget = self.min_budget*400 - current_lifetime_spend * 100
                else:
                    daily_budget = self.min_budget*100 - current_lifetime_spend*100
                for i in range(3):
                    res = facebook_api.update_adset_day(self.ad_set_id, daily_budget)
                    if res:
                        self.logger.info('日期变更后，预算设置成功，预算为: ' + str(daily_budget))
                        break
                    else:
                        print(self.ad_set_id, '日期变更后，预算设置失败')
                        self.logger.info('日期变更后，预算设置失败')
                    time.sleep(1)
                self.init_state = mongo.get_profile(self.ad_set_id)

            status = self.check_status()
            if status == 2:
                self.logger.info('请求重建adset')
                data = facebook_api.get_data(self.ad_set_id)
                if data:
                    targeting = data['targeting']
                    if current_lifetime_pay_times > 0:
                        mongo.insert_targeting(self.ad_set_id, targeting, 'true')
                    else:
                        mongo.insert_targeting(self.ad_set_id, targeting, 'false')
                    self.logger.info('获取旧adset的数据 成功')
                else:
                    print('获取旧adset的数据 失败')
                    self.logger.info('获取旧adset的数据 失败')

                try:
                    new_data = facebook_api.get_new_targeting(data)
                    new_adset_id = new_data['adset_id']
                    self.logger.info('获取新的adset成功，new_adset_id为:' + str(new_adset_id))
                    mongo.insert_profile_data(self.ad_set_id, new_adset_id)
                except Exception as e:
                    new_adset_id = self.ad_set_id
                    print(self.ad_set_id, '获取新的adset_id出错')
                    self.logger.info('获取新的adset_id出错')
                    daily_budget = self.min_budget * 100
                    res = facebook_api.update_adset_day(new_adset_id, daily_budget)
                    if res:
                        print(self.ad_set_id, '获取重建广告adset_id失败，让原来的继续跑一轮，故重置预算，成功，预算为: ' + str(daily_budget))
                        self.logger.info('获取重建广告adset_id失败，让原来的继续跑一轮，故重置预算，成功，预算为: ' + str(daily_budget))
                    else:
                        print(self.ad_set_id, '获取重建广告adset_id失败，让原来的继续跑一轮，故重置预算，但预算设置失败')
                        self.logger.info('获取重建广告adset_id失败，让原来的继续跑一轮，故重置预算，但预算设置失败')

                    req = facebook_api.set_bid_amount(self.ad_set_id, self.init_bid_amount)
                    if req:
                        print(self.ad_set_id, '获取重建广告adset_id失败，让原来的继续跑一轮，故重置出价，成功')
                        self.logger.info('获取重建广告adset_id失败，让原来的继续跑一轮，故重置出价，成功，new_bid_amount为: ' + str(self.init_bid_amount))
                    else:
                        print(self.ad_set_id, '获取重建广告adset_id失败，让原来的继续跑一轮，故重置出价，但出价设置失败')
                        self.logger.info('获取重建广告adset_id失败，让原来的继续跑一轮，故重置出价，但出价设置失败')
                    print(e)

                # 进行数据初始化
                self.ad_set_id = new_adset_id
                self.init_state = mongo.get_profile(self.ad_set_id)
                self.init_state_morning = mongo.get_profile(self.ad_set_id)
                self.init_state_status = mongo.get_profile(self.ad_set_id)
                self.current_budget = self.min_budget
                self.current_max_hours = self.min_hours
                self.is_cold = True
                self.phase = 'impression'
                self.cold_clocks = 0
                self.adjust_direction = 0
                self.last_e_value = 0
                self.bid_amount = 0
                self.logger = log.init_log(self.ad_set_id)

            if status == 3:
                print(self.ad_set_id, 'targeting phase end')
                self.logger.info('重组结束')
                data = facebook_api.get_data(self.ad_set_id)
                targeting = data['targeting']
                mongo.insert_targeting(self.ad_set_id, targeting, 'true')
                mongo.insert_good_adset_id(self.ad_set_id)
                res = facebook_api.update_adset_day(self.ad_set_id, self.min_budget)
                if res:
                    print(self.ad_set_id, '重组结束，预算设置成功，预算为: ' + str(self.min_budget))
                    self.logger.info('重组结束，预算设置成功，预算为: ' + str(self.min_budget))
                else:
                    print(self.ad_set_id, '重组结束，但预算设置失败')
                    self.logger.info('重组结束，但预算设置失败')
                break

            self.spend_control()
            time.sleep(300)


    def get_profile(self):
        profile = mongo.get_profile(self.ad_set_id)
        for key in profile:
            profile[key] = round(profile[key] - self.init_state[key], 2)
        return profile


    def get_today_spend(self):
        profile = mongo.get_profile(self.ad_set_id)
        today_spend = round(profile['spend'] - self.init_state_morning['spend'], 2)
        return today_spend


    def get_status_profile(self):
        profile = mongo.get_profile(self.ad_set_id)
        for key in profile:
            profile[key] = round(profile[key] - self.init_state_status[key], 2)
        return profile


    def check_status(self):
        # 返回状态码定义：
        # 1 - 正常状态
        # 2 - 定向重组
        # 3 - 完成该阶段任务，进入投放优化
        # 状态发生改变就要对出价进行重新设置，设置成初始出价:init_bid_amount
        profile = self.get_profile()
        current_lifetime_spend = profile['spend']
        current_lifetime_hours = profile['lifetime']/60
        current_lifetime_impressions = profile['impressions']
        current_lifetime_install = profile['install']
        global current_lifetime_pay_times
        current_lifetime_pay_times = profile['pay']
        current_lifetime_pay_users = profile['unique_pay']

        if self.phase == 'impression':
            # 如果达到目标，就转入下个阶段
            if current_lifetime_impressions >= self.impression_count:
                self.phase = 'install'
                self.current_budget = 2 * self.min_budget
                self.current_max_hours = 2 * self.min_hours
                self.init_state_status = mongo.get_profile(self.ad_set_id)
                res = facebook_api.set_bid_amount(self.ad_set_id, self.init_bid_amount)
                if res:
                    self.logger.info('该adset进入install阶段，并成功完成出价设置，new_bid_amount为: ' + str(self.init_bid_amount))
                else:
                    print(self.ad_set_id, '该adset进入install阶段，但出价设置失败')
                    self.logger.info('该adset进入install阶段，但出价设置失败')

                self.bid_amount = self.init_bid_amount
                for i in range(3):
                    res = facebook_api.update_phase(self.ad_set_id, self.min_budget)
                    if res:
                        self.logger.info('该adset进入install阶段，并成功完成预算设置')
                        break
                    else:
                        print(self.ad_set_id, '该adset进入install阶段，但预算设置失败')
                        self.logger.info('该adset进入install阶段，但预算设置失败')
                    time.sleep(1)
                return 1
            # 否则，判断预算
            elif current_lifetime_spend >= self.current_budget:
                self.logger.info('因为阶段花费用完了还没达标，故请求重建')
                return 2
            # 如果花费超出预算，则进入定向重组程序
            elif current_lifetime_hours >= self.current_max_hours:
                self.logger.info('因为阶段时长用完了还没达标，故请求重建')
                return 2
            else:
                return 1
            # 如果花费小于预算，则返回正常状态，继续学习
        elif self.phase == 'install':
            if current_lifetime_install >= self.install_count:
                self.phase = 'pay_times'
                self.current_budget = 3 * self.min_budget
                self.current_max_hours = 3 * self.min_hours
                self.init_state_status = mongo.get_profile(self.ad_set_id)
                res = facebook_api.set_bid_amount(self.ad_set_id, self.init_bid_amount)
                if res:
                    self.logger.info('该adset进入pay_times阶段，并成功完成出价设置，new_bid_amount为: ' + str(self.init_bid_amount))
                else:
                    print(self.ad_set_id, '该adset进入pay_times阶段，但出价设置失败')
                    self.logger.info('该adset进入pay_times阶段，但出价设置失败')
                self.bid_amount = self.init_bid_amount
                for i in range(3):
                    res = facebook_api.update_phase(self.ad_set_id, self.min_budget)
                    if res:
                        self.logger.info('该adset进入pay_times阶段，并成功完成预算设置')
                        break
                    else:
                        print(self.ad_set_id, '该adset进入pay_times阶段，但预算设置失败')
                        self.logger.info('该adset进入pay_times阶段，但预算设置失败')
                    time.sleep(1)
                return 1
            elif current_lifetime_spend >= self.current_budget:
                self.logger.info('因为阶段花费用完了还没达标，故请求重建')
                return 2
            elif current_lifetime_hours >= self.current_max_hours:
                self.logger.info('因为阶段时长用完了还没达标，故请求重建')
                return 2
            else:
                return 1
        elif self.phase == 'pay_times':
            if current_lifetime_pay_times >= self.pay_times_count:
                self.phase = 'pay_users'
                self.current_budget = 4 * self.min_budget
                self.current_max_hours = 4 * self.min_hours
                self.init_state_status = mongo.get_profile(self.ad_set_id)
                res = facebook_api.set_bid_amount(self.ad_set_id, self.init_bid_amount)
                if res:
                    self.logger.info('该adset进入pay_users阶段，并成功完成出价设置，new_bid_amount为: ' + str(self.init_bid_amount))
                else:
                    print(self.ad_set_id, '该adset进入pay_users阶段，但出价设置失败')
                    self.logger.info('该adset进入pay_users阶段，但出价设置失败')
                self.bid_amount = self.init_bid_amount
                for i in range(3):
                    res = facebook_api.update_phase(self.ad_set_id, self.min_budget)
                    if res:
                        self.logger.info('该adset进入pay_users阶段，并成功完成预算设置')
                        break
                    else:
                        print(self.ad_set_id, '该adset进入pay_users阶段，但预算设置失败')
                        self.logger.info('该adset进入pay_users阶段，但预算设置失败')
                    time.sleep(1)
                return 1
            elif current_lifetime_spend >= self.current_budget:
                self.logger.info('因为阶段花费用完了还没达标，故请求重建')
                return 2
            elif current_lifetime_hours >= self.current_max_hours:
                self.logger.info('因为阶段时长用完了还没达标，故请求重建')
                return 2
            else:
                return 1
        elif self.phase == 'pay_users':
            if current_lifetime_pay_users >= self.pay_users_count:
                res = facebook_api.set_bid_amount(self.ad_set_id, self.init_bid_amount)
                if res:
                    print(self.ad_set_id, '该adset完成重组过程，并成功完成出价设置')
                    self.logger.info('该adset完成重组过程，并成功完成出价设置，new_bid_amount为: ' + str(self.init_bid_amount))
                else:
                    print(self.ad_set_id, '该adset完成重组过程，但出价设置失败')
                    self.logger.info('该adset完成重组过程，但出价设置失败')
                self.bid_amount = self.init_bid_amount
                return 3
            elif current_lifetime_spend >= self.current_budget:
                self.logger.info('因为阶段花费用完了还没达标，故请求重建')
                return 2
            elif current_lifetime_hours >= self.current_max_hours:
                self.logger.info('因为阶段时长用完了还没达标，故请求重建')
                return 2
            else:
                return 1
