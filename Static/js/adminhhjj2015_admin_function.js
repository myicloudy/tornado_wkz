function confirmurl(url,message) {
	window.top.art.dialog.confirm(message, function(){
    	redirect(url);
	}, function(){
	    return true;
	});
	//if(confirm(message)) redirect(url);
}

function redirect(url) {
	location.href = url;
}

/**
* 删除单个数据
* author			: gg
* create date		: 2016-03-12
* @param id			: 删除数据的id
* @param promptText	: 提示语
* @param delUrl		: 删除数据的url
*/
function delOneId(id,promptText,delUrl){
	if(id == "select"){
		var selectIdStrs		= ""
		$(".trdata").each(function(index, element) {
			if($(this).find("input[type='checkbox']").prop("checked")){ 
				if(selectIdStrs != ""){
					selectIdStrs	+= ",";   
				}
				selectIdStrs		+= $(this).find("input[type='checkbox']").val()
			};
		});	
		id		= selectIdStrs;
		if(id == ""){
			window.top.art.dialog({id:'msgbox',content:"未选中任何需要删除的内容",lock:true,width:250,height:100}).time(2);
			return false;
		}
	}
	window.parent.art.dialog.confirm(promptText,function(){	
		$.ajax({
			url:delUrl,
			type:"POST",
			dataType:"json",
			data:"id="+id,
			success: function(msg){
				window.parent.art.dialog({id:'msgbox',content:msg.info,lock:true,width:250,height:100}).time(2);
				if(msg.status == 1){
					window.location.reload();
					return false;
				}
			}	
		});					
	});
}

/**
* 全选和取消全选
* author			: zhubo
* create date		: 2015-07-06
* @param me			: 当前选择的对象
*/
function selectAll(me){
	if($(me).prop("checked")){
		$(".trdata").each(function(index, element) {
			$(this).find("input[type='checkbox']").prop("checked",true); 
		});
	}else{
		$(".trdata").find("input[type='checkbox']").prop("checked",false);
	}
}
/**
* 弹出修改窗口
* author			: gg
* create date		: 2016-03-12
* @param Url		: 弹窗url
* @param Url		: 弹窗标题
*/
function openEditWindow(Url, name) {
	window.art.dialog.open(Url,{id:'openEditWindow',title: name, width: '80%', height: '80%',close: function() {location.reload();}});
}