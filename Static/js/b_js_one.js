
	var x_shuju='';
function add_bshuju(tvalue,fdsfds){
		if (fdsfds=='1'){
			if (x_shuju==''){			
				x_shuju=tvalue;
			}else{
				x_shuju=x_shuju+","+tvalue;
			}
			iner_shuju(x_shuju);
		}
	}
	function del_bshuju(vv){
		x_shuju=x_shuju.replace(vv,"");
		iner_shuju(x_shuju);
	}
	function add_sortid(vv){
		//alert('fff'+vv);
		document.all.sortid.value=vv;
	}
	function del_sortid(){
		document.all.sortid.value='';
	}
	function iner_shuju(x_shuju){
		var d_shuju=x_shuju.split(",");
		
			d_html="";
			for(i=0;i<d_shuju.length;i++){
				if (d_shuju[i]!='')
				{
					var d_s_shuju=d_shuju[i].split("||");
					d_html=d_html+d_s_shuju[1]+"(<font color=red><span onclick=del_bshuju('"+d_shuju[i]+"')>删</span></font>)";
				}
				
			}
			//$("#shuji_l").html(d_html);
			//alert(d_html);
			document.getElementById('fdseeee').innerHTML=d_html;
	}
	