import requests
import json
import time
import urllib3

# 禁用SSL验证（仅用于测试环境）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 创建会话，复用连接
session = requests.Session()
session.verify = False  # 禁用SSL验证

def get_geocode(address, key):
    """
    根据地址获取地理坐标
    :param address: 地址字符串
    :param key: 高德地图API密钥
    :return: 包含经纬度的字典
    """
    url = "https://restapi.amap.com/v3/geocode/geo"
    params = {
        "key": key,
        "address": address
    }
    
    try:
        print(f"正在获取 {address} 的坐标...")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()  # 检查HTTP响应状态
        data = response.json()
        
        if data["status"] == "1" and len(data["geocodes"]) > 0:
            location = data["geocodes"][0]["location"]
            lng, lat = location.split(",")
            result = {
                "address": address,
                "longitude": float(lng),
                "latitude": float(lat),
                "formatted_address": data["geocodes"][0]["formatted_address"]
            }
            print(f"成功获取坐标: {result}")
            return result
        else:
            error_msg = f"未找到该地址的坐标，错误信息: {data.get('info', '未知错误')}"
            print(error_msg)
            return {
                "address": address,
                "error": error_msg
            }
    except Exception as e:
        error_msg = f"请求失败: {str(e)}"
        print(error_msg)
        return {
            "address": address,
            "error": error_msg
        }

def search_poi(keyword, city, key, page=1, page_size=25):
    """
    搜索POI（兴趣点）
    :param keyword: 搜索关键词
    :param city: 城市名称
    :param key: 高德地图API密钥
    :param page: 页码
    :param page_size: 每页结果数
    :return: POI列表
    """
    url = "https://restapi.amap.com/v3/place/text"
    params = {
        "key": key,
        "keywords": keyword,
        "city": city,
        "page": page,
        "offset": page_size,
        "extensions": "base"
    }
    
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data["status"] == "1":
            pois = data.get("pois", [])
            print(f"第{page}页搜索成功，找到 {len(pois)} 个结果")
            return pois
        else:
            error_msg = f"POI搜索失败: {data.get('info', '未知错误')}"
            print(error_msg)
            return []
    except Exception as e:
        error_msg = f"POI搜索请求失败: {str(e)}"
        print(error_msg)
        return []

def get_all_pois(keyword, city, key, max_pages=10):
    """
    获取所有POI结果
    :param keyword: 搜索关键词
    :param city: 城市名称
    :param key: 高德地图API密钥
    :param max_pages: 最大页码
    :return: 所有POI列表
    """
    all_pois = []
    page = 1
    
    while page <= max_pages:
        print(f"正在搜索第{page}页...")
        pois = search_poi(keyword, city, key, page=page)
        
        if not pois:
            break
        
        all_pois.extend(pois)
        page += 1
        time.sleep(1)  # 避免请求过于频繁
    
    return all_pois

def batch_geocode(addresses, key, delay=1):
    """
    批量获取地理坐标
    :param addresses: 地址列表
    :param key: 高德地图API密钥
    :param delay: 请求间隔（秒）
    :return: 结果列表
    """
    results = []
    for address in addresses:
        result = get_geocode(address, key)
        results.append(result)
        time.sleep(delay)  # 避免请求过于频繁
    return results

def save_results(results, filename):
    """
    保存结果到JSON文件
    :param results: 结果列表
    :param filename: 文件名
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

def filter_communities(community_list):
    """过滤小区列表，只保留真正的住宅小区"""
    # 排除的关键词
    exclude_keywords = ["小商品", "商场", "批发市场", "超市", "店", "批发", "市场", "驾校", "医院", "学校", "酒店", "宾馆", "饭店", "餐厅", "网吧", "KTV", "酒吧", "影院", "剧场", "体育馆", "运动场", "公园", "景区", "景点", "博物馆", "图书馆", "政府", "机关", "单位", "公司", "企业", "工厂", "车间", "仓库", "物流", "快递", "车站", "码头", "机场", "加油站", "加气站", "停车场", "修理厂", "4S店", "汽车", "建材", "家具", "电器", "数码", "服装", "鞋帽", "箱包", "饰品", "化妆品", "医药", "药店", "医院", "诊所", "药店", "银行", "保险", "证券", "金融", "投资", "理财", "房产", "中介", "装修", "装饰", "建材", "家具", "电器", "数码", "服装", "鞋帽", "箱包", "饰品", "化妆品", "医药", "药店", "医院", "诊所", "药店", "银行", "保险", "证券", "金融", "投资", "理财", "房产", "中介", "装修", "装饰"]
    
    # 保留的关键词
    include_keywords = ["小区", "花园", "苑", "园", "庄", "城", "里", "坊", "院", "府", "邸", "居", "舍", "公寓", "别墅", "住宅", "家园", "佳园", "嘉园", "花园", "花苑", "山庄", "别墅", "公馆", "府邸", "公寓", "商住楼", "住宅楼"]
    
    filtered_list = []
    for community in community_list:
        name = community["name"]
        
        # 检查是否包含排除关键词
        exclude = False
        for keyword in exclude_keywords:
            if keyword in name:
                exclude = True
                break
        if exclude:
            continue
        
        # 检查是否包含保留关键词
        include = False
        for keyword in include_keywords:
            if keyword in name:
                include = True
                break
        if include:
            filtered_list.append(community)
    
    return filtered_list

if __name__ == "__main__":
    # 替换为你的高德地图API密钥
    api_key = "ed5499e558ca8836915a6f077a95fdb3"
    
    try:
        # 搜索福州市的小区，使用更广泛的关键词
        print("开始搜索福州市小区...")
        pois = get_all_pois("小区", "福州", api_key, max_pages=20)  # 搜索20页，获取更多结果
        
        print(f"共找到 {len(pois)} 个小区")
        
        # 提取小区名称和地址
        community_info = []
        for poi in pois:
            name = poi.get("name", "")
            address = poi.get("address", "")
            if name:
                # 如果有地址，使用完整地址；否则使用城市+小区名
                full_address = f"福州市{address}" if address else f"福州市{name}"
                community_info.append({"name": name, "address": full_address})
        
        # 去重，根据小区名字
        unique_communities = {}
        for info in community_info:
            if info["name"] not in unique_communities:
                unique_communities[info["name"]] = info
        
        # 转换为列表
        unique_community_list = list(unique_communities.values())
        print(f"去重后共 {len(unique_community_list)} 个小区")
        
        # 过滤小区，只保留真正的住宅小区
        filtered_communities = filter_communities(unique_community_list)
        print(f"过滤后共 {len(filtered_communities)} 个真正的住宅小区")
        
        # 批量获取坐标
        print("开始获取地理坐标...")
        addresses = [info["address"] for info in filtered_communities]
        results = batch_geocode(addresses, api_key)
        
        # 合并小区名字和坐标信息
        final_results = []
        for i, result in enumerate(results):
            if i < len(filtered_communities):
                final_result = {
                    "name": filtered_communities[i]["name"],
                    "address": result["address"],
                    "longitude": result.get("longitude"),
                    "latitude": result.get("latitude"),
                    "formatted_address": result.get("formatted_address"),
                    "error": result.get("error")
                }
                final_results.append(final_result)
        
        # 保存结果到文件
        save_results(final_results, "fuzhou_communities_geocode.json")
        print("结果已保存到 fuzhou_communities_geocode.json")
        
        # 打印部分结果
        print("\n部分结果示例:")
        for i, result in enumerate(final_results[:10]):
            print(f"{i+1}. {json.dumps(result, ensure_ascii=False)}")
            
    except Exception as e:
        print(f"程序执行失败: {str(e)}")
        import traceback
        traceback.print_exc()
