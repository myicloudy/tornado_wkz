class Pagination:
    def __init__(self, current_page, all_item, base_url='', per_pagenum=5, min_per_pagenum=1, max_per_pagenum=100):
        # 每页条目数量
        try:
            per_pagenum = int(per_pagenum)
            if per_pagenum < min_per_pagenum:
                self._per_pagenum = min_per_pagenum
            elif per_pagenum > max_per_pagenum:
                self._per_pagenum = max_per_pagenum
            else:
                self._per_pagenum = per_pagenum
        except:
            self._per_pagenum = 5

        # 总共页数
        try:
            all_item = int(all_item)
            self._count = all_item
            self._p_nums, c = divmod(all_item, self._per_pagenum)
            if c > 0:
                self._p_nums += 1
        except Exception as e:
            raise e

        # 当前页码
        try:
            page = int(current_page)
            if page < 1:
                self._current_page = 1
            elif page > self._p_nums:
                self._current_page = self._p_nums
            else:
                self._current_page = page
        except:
            self._current_page = 1

        # 基础URL
        self.base_url = base_url

    @property
    def page(self):
        """
        :return: 返回当前页页码
        """
        return self._current_page

    @property
    def num_per_page(self):
        """
        :return: 每页显示项个数
        """
        return self._per_pagenum

    @property
    def count(self):
        """
        :return: 所有项个数
        """
        return self._count

    @property
    def num_pages(self):
        """
        :return:返回总页数.
        """
        return self._p_nums

    def has_prev(self):
        """
        :return: 是否有上一页
        """
        if self._current_page == 1:
            return False
        else:
            return True

    def has_next(self):
        """
        :return: 是否有下一页
        """
        if self._current_page == self._p_nums:
            return False
        else:
            return True

    @property
    def start_index(self):
        """
        :return: 当前页起始条目索引值
        """
        return (self._current_page - 1) * self._per_pagenum

    @property
    def end_index(self):
        """
        :return: 当前页终止条目索引值
        """
        return self._current_page * self._per_pagenum

    # 显示前端分页效果
    def show_pager(self):
        list_page = []
        if self._p_nums < self._per_pagenum:
            s = 1
            t = self._p_nums + 1
        else:  # 总页数大于11
            if self._current_page < 6:
                s = 1
                t = 12
            else:
                if (self._current_page + 5) < self._p_nums:
                    s = self._current_page - 5
                    t = self._current_page + 5 + 1
                else:
                    s = self._p_nums - self._per_pagenum
                    t = self._p_nums + 1
        # 首页
        first = '<li><a href="/%s?page=1" >首页</a></li>' % self.base_url
        list_page.append(first)
        # 上一页
        if self.has_prev():
            prev = '<li><a href="/%s?page=%s">上一页</a></li>' % (self.base_url, self._current_page - 1,)
            list_page.append(prev)
        # 显示个页码
        for p in range(s, t):  # 1-11
            if p == self._current_page:
                temp = '<li><a class="active" href="/%s?page=%s" >%s</a></li>' % (self.base_url, p, p)
            else:
                temp = '<li><a href="/%s?page=%s">%s</a></li>' % (self.base_url, p, p)
            list_page.append(temp)
        # 下一页
        if self.has_next():
            next = '<li><a href="/%s?page=%s">下一页</a></li>' % (self.base_url, self._current_page + 1,)
            list_page.append(next)

        # 尾页
        last = '<li><a href="/%s?page=%s">尾页</a></li>' % (self.base_url, self._p_nums,)
        list_page.append(last)

        # 跳转
        # jump = """<input type='text' style="height: 25px;width: 40px;"/><li><a onclick="Jump('%s?page=',this);">跳转</a></li>""" % (self.base_url)
        # script = """<script>
        #     function Jump(baseUrl,ths){
        #         var val = ths.previousElementSibling.value;
        #         if(val.trim().length>0){
        #             location.href = baseUrl + val;
        #         }
        #     }
        #     </script>"""
        # list_page.append(jump)
        # list_page.append(script)
        str_page = "".join(list_page)
        return str_page


if __name__ == '__main__':
    p = Pagination(2, 100, 'index', per_pagenum=10)
    print(p._p_nums, p.base_url, p._current_page)
