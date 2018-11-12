#整个项目的配置值
from .DefaultValue import value
from .Analysis import Analysis_Default
from .Message import Message_Default
from .WeixinUpdate import WeixinUpdate_Default
from .AliPay import AliPay_Default

DefaultValues={**value,**Analysis_Default,**Message_Default,**WeixinUpdate_Default,**AliPay_Default}
