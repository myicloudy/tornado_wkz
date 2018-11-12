from Controller.Crontab.CommonCrontab import CommonCrontab


class CheckParams(CommonCrontab):
    def null_params(self, dic):
        """
        非空判断
        :param dic: 传入的参数
        :return: key
        """
        self.AppLogging.info('null_params')
        for key in dic:
            if dic[key] == '':
                return key

    def yz_minute(self, para, start, end):
        """
        判断传入参数是否合法
        :param para:
        :param start:
        :param end:
        :return: 此参数 or 错误标识
        """
        enm = [str(ch) for ch in range(10)] + [',', '*', '/']
        if set(para).issubset(set(enm)):  # 判断是否有不合法字符  集合 子集
            if ',' in para:  # 判断含有','的多个参数
                c1 = para.split(',')
                if len(c1) > 10:  # 判断传入的长度
                    return 30006
                for ch in c1:  # 循环得到每一个
                    if len(ch) < 3:  # 如果为数字
                        if not set(c1).issubset(set([str(i) for i in range(start, end)])):
                            return 30007
                    elif len(ch) > 2:
                        return 30007
                    # if '/' in ch:  # 判断是否有'/'
                    #     c2 = ch.split('/')
                    #     if c2[0] != '*' or c2[1] not in [str(i) for i in range(start, end)]:
                    #         return 30009
                return para
            elif '/' in para:  # 判断单独含有'/'时
                c2 = para.split('/')
                if c2[0] == '*' and c2[1] in [str(i) for i in range(start, end)]:
                    return para
                return 30009
            if para in [str(i) for i in range(start, end)]:  # 仅仅为单独数字时
                return para
            else:
                return 30007
        return 30008

    def yz_hours_to_week(self, para, start, end):
        """
        判断传入参数是否合法
        :param para:
        :param start:
        :param end:
        :return: 此参数 or 错误标识
        """
        enm = [str(ch) for ch in range(10)] + [',', '*', '/', '-']
        if set(para).issubset(set(enm)):  # 判断是否有不合法字符
            if ',' in para:  # 判断含有','的多个参数
                c1 = para.split(',')
                if len(c1) > 10:  # 判断传入的长度
                    return 30006
                for ch in c1:  # 循环得到每一个
                    if len(ch) < 3:  # 如果为数字
                        if not set(c1).issubset(set([str(i) for i in range(start, end)])):
                            return 30007
                    elif len(ch) > 2:
                        return 30007
                    # if '/' in ch:  # 判断是否有'/'
                    #     c2 = ch.split('/')
                    #     if c2[0] != '*' or c2[1] not in [str(i) for i in range(start, end)]:
                    #         return 30009
                    if '-' in ch:
                        c3 = ch.split('-')
                        if len(c3) == 2:
                            try:
                                if int(c3[0]) < int(c3[1]) and c3[1] in [str(i) for i in range(start, end)]:
                                    return para
                            except ValueError as e:
                                return 30007
                        return 30009
                return para
            elif '/' in para:  # 判断单独含有'/'时
                c2 = para.split('/')
                if c2[0] == '*' and c2[1] in [str(i) for i in range(start, end)]:
                    return para
                return 30009
            elif '-' in para:  # 判断单独含有'-'时
                c3 = para.split('-')
                if len(c3) == 2:
                    try:
                        if int(c3[0]) < int(c3[1]) and c3[1] in [str(i) for i in range(start, end)]:
                            return para
                    except ValueError as e:
                        return 30009
                return 30007
            elif '*' in para:  # 判断单独含有'*'时
                if len(para) == 1:
                    return para
                return 30009
            if para in [str(i) for i in range(start, end)]:  # 仅仅为单独数字时
                return para
            else:
                return 30007
        return 30008
