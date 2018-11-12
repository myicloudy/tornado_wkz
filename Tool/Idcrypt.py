# coding:utf-8
# @Explain : id加密
# @Time    : 2018/8/22 上午11:31
# @Author  : gg
# @FileName: Idcrypt.py
# 加密规则说明：
# 加密方案，根据 秘钥编排算法，由一个32比特秘钥生成5个16比特子秘钥 4轮的SPN网络，可以用来进行加密或解密 简称秘钥算法
# 加密策略：
# 1.本加密位由秘钥算法得到，只支持4位以内数字进行加密(可以保证数据解密后的正确性)
# 2.当传来一个值时，首先判断传来的值是否为整型，如果不为整型则直接返回None
# 3.否则将传来的值进行转化为字符串，判断字符串的长度，如果不为4或者4的倍数则进行补零方法
# 4.补零方法：例如 10003 补零后为 0001 0003，将这两段字符串再转化为int类型进行秘钥算法得到密文
# 5.在得到密文的基础上进一步加密，将加密后的每个数字，从小写字母a对应的ascii码97算起，之后97+数字，chr得到对应的字母 加密显示的字母范围 a-j
# 6.为了便于解密将每段进一步加密后的字符串用'k'插入，分段拼接，因为秘钥算法只支持4位加密
# 7.在验证过程中如果是10003，根据补位得出0001 0003，发现后4位后出现0003，会变成3进入秘钥算法加密后解密得出是3，去确定其内部0的个数和位数，增加一位标志位算法
#   标识位算法处理方法：
#       7.1 第一个4位不用处理
#       7.2 0001表示4位中有3个0，二进制转换为十进制值为1
#       7.3 0011表示4位中有2个0，二进制转换为十进制值为3
#       7.4 0111表示4位中有1个0，二进制转换为十进制值为7
#       7.5 1111表示4位中有0个0，二进制转换为十进制值为15
#       7.6 根据上述得出十进制后转换为字母，转换规则：从小写字母a对应的ascii码97算起 之后97+数字 chr得到对应的字母 有可能出现的值为 a b g p
#       7.7 将得出字母拼接到分隔符'k'后面，例如得到密文信息 例如： dbdca ka cdedi kb ddiig k为分割符
# 8.每4个加密后连接分隔符'k'+标识位得出最后加密字符串

# 解密策略：
# 1.解密时，会传入这样一段密文 例如 dbdca ka cdedi kb ddiig，根据之前规定的分割符k, k后为标识位
# 2.根据k进行分割，得到每一段密文，第一段需要单独处理，因为这段加密时不需要标识位，所以也不需要根据标识位进行截取得到每一段的密文
# 3.如果为第一段密文，进行转换ord()-97得到每一个字符的数字，然后再进行秘钥算法得到解密值
# 4.第二段密文以后的第一位为标识位
#   标识位算法处理方法：
#       4.1 截取每一段的第一位字母，进行转换ord()-97得到数字，转换为二进制
#       4.2 十进制值为1转换为二进制0001表示4位中有3个0
#       4.3 十进制值为3转换为二进制0011表示4位中有2个0
#       4.4 十进制值为7转换为二进制0111表示4位中有1个0
#       4.5 十进制值为15转换为二进制1111表示4位中有0个0
# 5.以分隔符'k'分割后数据解密后连接上并转化为字符串返回。非数字类型


class Idcrypt():
    def __init__(self, K=0b00101010100101001001011000101101):
        # S盒参数
        self.S_Box = [14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7]
        self.K = K

        # P盒参数
        self.P_Box = [1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15, 4, 8, 12, 16]

    def gen_K_list(self):
        """
        秘钥编排算法，由一个32比特秘钥生成5个16比特子秘钥
        :param K: 32比特秘钥
        :return: [k1,k2,k3,k4,k5]，五个16比特子秘钥
        """
        K = self.K
        Ks = []
        for i in range(5, 0, -1):
            ki = K % (2 ** 16)
            Ks.insert(0, ki)
            K = K >> 4
        return Ks

    def pi_s(self, s_box, ur):
        """
        分组代换操作
        :param s_box:S盒参数
        :param ur:输入比特串，16比特
        :return:输出比特串，16比特
        """
        vr = 0
        for i in range(4):
            uri = ur % (2 ** 4)
            vri = s_box[uri]
            vr = vr + (vri << (4 * i))
            ur = ur >> 4
        return vr

    def pi_p(self, p_box, vr):
        """
        单比特置换操作
        :param p_box:P盒参数
        :param vr:输入比特串，16比特
        :return:输出比特串，16比特
        """
        wr = 0
        for i in range(15, -1, -1):
            vri = vr % 2
            vr = vr >> 1
            wr = wr + (vri << (16 - p_box[i]))
        return wr

    def reverse_Sbox(self, s_box):
        """
        求S盒的逆
        :param s_box:S盒参数
        :return:S盒的逆
        """
        re_box = [-1] * 16
        for i in range(16):
            re_box[s_box[i]] = i
        return re_box

    def reverse_Pbox(self, p_box):
        """
        求P盒的逆
        :param s_box:P盒参数
        :return:P盒的逆
        """
        re_box = [-1] * 16
        for i in range(16):
            re_box[p_box[i] - 1] = i + 1
        return re_box

    def do_SPN(self, x, s_box, p_box, Ks):
        """
        4轮的SPN网络，可以用来进行加密或解密
        :param x: 16比特输入
        :param s_box: S盒参数
        :param p_box: P盒参数
        :param Ks: [k1,k2,k3,k4,k5]，五个16比特子秘钥
        :return: 16比特输出
        """
        wr = x
        for r in range(3):
            ur = wr ^ Ks[r]  # 异或操作
            vr = self.pi_s(s_box, ur)  # 分组代换
            wr = self.pi_p(p_box, vr)  # 单比特置换

        ur = wr ^ Ks[3]
        vr = self.pi_s(s_box, ur)
        y = vr ^ Ks[4]
        return y

    def encrypt(self, x):
        """
        根据秘钥K对16比特明文x进行加密
        :param K:32比特秘钥
        :param x:16比特明文
        :return:16比特密文
        """
        x=str(x)
        Ks = self.gen_K_list()
        if x.isdigit():  # 例如 100003  判断是否为数字
            rslist = []
            for ch in range(len(x)):  # 将数字转化为字符串进行一系列的处理
                if ch % 4 == 0:
                    rslist.append(x[::-1][ch:ch + 4])
            rslist[-1] = rslist[-1] + '0' * (4 - len(rslist[-1]))
            rslist = [ch[::-1] for ch in rslist]
            rslist.reverse()  # rslist  ['0010', '0003']
            jmstr = ''
            ksy = 0  # 标识位  主要针对补零策略 加在k的后面
            for ch in rslist:
                if int(ch) == 0:
                    ksy = 0  # 根据二进制  0000  ksy = 0
                elif len(str(int(ch))) == 1:
                    ksy = 1  # 根据二进制  0001  ksy = 1
                elif len(str(int(ch))) == 2:
                    ksy = 3  # 根据二进制  0011  ksy = 3
                elif len(str(int(ch))) == 3:
                    ksy = 7  # 根据二进制  0111  ksy = 7
                else:
                    ksy = 15  # 根据二进制  1111  ksy = 15
                y = self.do_SPN(int(ch), self.S_Box, self.P_Box, Ks)  # 得到每一段的一级密文
                ylist = [chr(int(s) + 97) for s in str(y)]  # 密文转码得到二级密文
                jmstr = jmstr + 'k' + chr(97 + ksy) + ''.join(ylist)  # 在密文中加入分隔符和标识符
            return jmstr[2:]  # 返回除第一个分隔符和标识符的字符串
        else:
            return None  # 如果不符合返回空

    def decrypt(self, y):
        """
        根据秘钥K对16比特密文y进行解密。
        :param K:32比特秘钥
        :param y:16比特密文
        :return:16比特明文
        """

        Ks = self.gen_K_list()
        Ks.reverse()  # 秘钥逆序编排
        # 秘钥置换
        Ks[1] = self.pi_p(self.P_Box, Ks[1])
        Ks[2] = self.pi_p(self.P_Box, Ks[2])
        Ks[3] = self.pi_p(self.P_Box, Ks[3])

        s_rbox = self.reverse_Sbox(self.S_Box)  # S盒求逆
        p_rbox = self.reverse_Pbox(self.P_Box)  # P盒求逆

        ylist = y.split('k')  # 通过分隔符得到密文  例如 y = cccchkbddiig
        zy = ''  # 初始化一个字符串，装载最终的数据
        ii = 0  # 初始化一个变量，控制循环，处理数据
        for ch in ylist:  # 循环每段密文
            if ii < 1:
                num = [str(ord(i) - 97) for i in ch]  # 第一段不需要标识符直接单独处理
                ss = self.do_SPN(int(''.join(num)), s_rbox, p_rbox, Ks)  # 密文转数字
            else:
                num = [str(ord(i) - 97) for i in ch[1:]]  # 之后的密文处理
                ss = self.do_SPN(int(''.join(num)), s_rbox, p_rbox, Ks)
                ss = str(ss)
                ksy = ord(ch[0]) - 97  # 根据标识符进行数据处理
                if ksy == 0:
                    ss = '0000'
                elif ksy == 1:
                    ss = "000" + ss
                elif ksy == 3:
                    ss = '00' + ss
                elif ksy == 7:
                    ss = '0' + ss
            zy += str(ss)
            ii += 1
        return zy  # 返回字符串

        # return self.do_SPN(y, s_rbox, p_rbox, Ks)
