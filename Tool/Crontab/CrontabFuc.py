from Controller.Crontab.CommonCrontab import CommonCrontab
from crontab import CronTab


class Mycron(CommonCrontab):
    def __init__(self, user=True):
        self.user = user
        self.crontab = CronTab(user=self.user)  # user=True对所有linux系统都适用

    def insertcron(self, shedule, shell, comment):
        """
        :param shedule: 任务指令
        :param shell: 执行脚本文件的命令
        :param comment: 任务名称
        :return: 0返回成功， 30000返回失败
        """
        self.AppLogging.info('insertcrontab')
        try:
            job = self.crontab.new(command=shell, comment=comment)
            job.setall(shedule)
            self.crontab.write()
            self.AppLogging.info(job)
            return 0
        except Exception as e:
            self.AppLogging.error(e)
            return 30000

    def look_crontab(self):
        """
        查看所有cron任务
        :return: 任务列表
        """
        self.AppLogging.info('lookcrontab')
        if self.crontab:
            self.AppLogging.info(self.crontab)
            return self.crontab
        return 1

    def find_for_nickname(self, nickname):
        """
        查找昵称是否存在
        :param nickname: 注释昵称
        :return: 30001存在  30002不存在
        """
        self.AppLogging.info('findfornickname')
        if nickname in self.crontab.comments:
            return 30001
        else:
            return 30002

    def find_one_nickname(self, nickname):
        """
        通过nickname查寻到其中一条任务，并返回此条任务
        :param nickname: 任务昵称
        :return: 一条任务或者一个错误数字30002
        """
        self.AppLogging.info('findonenickname')
        for job in self.crontab:
            if job.comment == nickname:
                self.AppLogging.info(job)
                return job
        else:
            return 30002

    def updcrontab(self, shedule, comment):
        """
        通过创建cron作业时的注释进行修改
        :param nickname: 注释名称
        :param *shedule: 任务指令
        :return: 0 成功  30000 写入失败
        """
        self.AppLogging.info('updcrontab')
        for job in self.crontab:
            if job.comment == comment:
                try:
                    job.setall(shedule)
                    self.crontab.write()
                    self.AppLogging.info(job)
                    return 0
                except Exception as e:
                    self.AppLogging.error(e)
                    return 30000

    def del_one_crontab(self, nickname):
        """
        通过创建cron作业时的注释进行删除
        :param nickname: 注释名称
        :return: 30003 成功 30004 失败
        """
        self.AppLogging.info('delonecrontab')
        try:
            self.crontab.remove_all(comment=nickname)
            self.crontab.write()
            return 30003
        except Exception as e:
            self.AppLogging.error(e)
            return 30004

    def del_all_crontab(self):
        """
        此功能并未在接口中实现
        清理所有工作的全部内容
        :return: ok or fail
        清理所有工作规则的工作
        self.crontab.clear()
        """
        try:
            self.crontab.remove_all()
            self.crontab.write()
            return 30003
        except Exception as e:
            self.AppLogging.error(e)
            return 30004
