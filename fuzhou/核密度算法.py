# -*- coding: utf-8 -*-
"""
核密度权重赋值 + 覆盖人口计算
"""
import arcpy
import numpy as np
import os

arcpy.env.overwriteOutput = True
arcpy.env.workspace = r"output.gdb"#补充完整路径

gdb = r"data.gdb"#补充完整路径
out_gdb = r"output.gdb"#补充完整路径
poi_fc = os.path.join(gdb, "研究区住宅区poi点")
store_fc = os.path.join(gdb, "朴朴超市门店点")
pop_raster = os.path.join(gdb, "福州市人口栅格")
study_area = os.path.join(gdb, "选址研究区域")
demand_pts = os.path.join(out_gdb, "需求点_聚类中心")
merged_area = os.path.join(out_gdb, "研究区合并")
pop_points = os.path.join(gdb, "福州市人口栅格转点")

# ========== 修复1: 核密度值提取到需求点 ==========
print("=" * 60)
print("步骤1: 从核密度图提取权重到需求点")
print("=" * 60)

kd_raster = os.path.join(out_gdb, "住宅区核密度")
demand_weighted = os.path.join(out_gdb, "需求点_带权重")

# 检查核密度栅格是否存在
if arcpy.Exists(kd_raster):
    print("核密度栅格存在，提取值到需求点...")
    try:
        # 使用arcpy.sa.ExtractValuesToPoints
        result = arcpy.sa.ExtractValuesToPoints(demand_pts, kd_raster, demand_weighted, "NONE", "VALUE_ONLY")
        # result是一个Raster对象，需要用save或直接用output路径
        demand_weighted = result
        print("ExtractValuesToPoints成功")
    except Exception as e:
        print(f"ExtractValuesToPoints失败: {e}")
        demand_weighted = None
else:
    print("核密度栅格不存在，跳过")
    demand_weighted = None

# 检查输出是否存在RASTERVALU字段
if demand_weighted and arcpy.Exists(str(demand_weighted)):
    fields = [f.name for f in arcpy.ListFields(str(demand_weighted))]
    if "RASTERVALU" in fields:
        # 归一化RASTERVALU作为权重
        values = []
        with arcpy.da.SearchCursor(str(demand_weighted), ["RASTERVALU"]) as cursor:
            for row in cursor:
                v = row[0]
                values.append(v if v is not None and v > -9999 else 0)
        values = np.array(values, dtype=float)
        min_v, max_v = values.min(), values.max()
        print(f"核密度值范围: {min_v:.6f} ~ {max_v:.6f}")
        if max_v > min_v:
            normalized = (values - min_v) / (max_v - min_v)
        else:
            normalized = np.ones_like(values)
        with arcpy.da.UpdateCursor(str(demand_weighted), ["Weight"]) as cursor:
            idx = 0
            for row in cursor:
                row[0] = round(normalized[idx], 6)
                cursor.updateRow(row)
                idx += 1
        print("核密度权重归一化完成")
    else:
        print("RASTERVALU字段不存在，回退到POI_Count")
        demand_weighted = None

if not demand_weighted or not arcpy.Exists(str(demand_weighted)):
    demand_weighted = os.path.join(out_gdb, "需求点_带权重")
    arcpy.management.CopyFeatures(demand_pts, demand_weighted)
    counts = []
    with arcpy.da.SearchCursor(demand_weighted, ["POI_Count"]) as cursor:
        for row in cursor:
            counts.append(row[0])
    counts = np.array(counts, dtype=float)
    min_c, max_c = counts.min(), counts.max()
    normalized = (counts - min_c) / (max_c - min_c) if max_c > min_c else np.ones_like(counts)
    with arcpy.da.UpdateCursor(demand_weighted, ["Weight"]) as cursor:
        idx = 0
        for row in cursor:
            row[0] = round(normalized[idx], 6)
            cursor.updateRow(row)
            idx += 1
    print("已使用POI_Count归一化作为权重")

# 打印权重统计
weights = []
with arcpy.da.SearchCursor(demand_weighted, ["Weight"]) as cursor:
    for row in cursor:
        weights.append(row[0])
weights = np.array(weights)
print(f"权重统计: 最小={weights.min():.4f}, 最大={weights.max():.4f}, 平均={weights.mean():.4f}")

# ========== 修复2: 正确计算覆盖人口 ==========
print("\n" + "=" * 60)
print("步骤2: 计算现有前置仓覆盖人口")
print("=" * 60)

buffer_dissolved = os.path.join(out_gdb, "现有前置仓_3km缓冲区_合并")

# 使用空间连接，但这次用 Join_count 来确认
print("使用空间连接判断覆盖...")

# 方法：用 SelectLayerByLocation 选择在缓冲区内的人口点
pop_layer = "pop_points_layer"
buffer_layer = "buffer_layer"
arcpy.management.MakeFeatureLayer(pop_points, pop_layer)
arcpy.management.MakeFeatureLayer(buffer_dissolved, buffer_layer)

# 选择在缓冲区内的人口点
arcpy.management.SelectLayerByLocation(pop_layer, "WITHIN", buffer_layer, "", "NEW_SELECTION")
covered_count = int(arcpy.management.GetCount(pop_layer).getOutput(0))

# 清除选择，计算研究区内人口点
arcpy.management.SelectLayerByLocation(pop_layer, "WITHIN", merged_area, "", "NEW_SELECTION")
total_count_in_study = int(arcpy.management.GetCount(pop_layer).getOutput(0))

# 覆盖人口值
covered_pop_value = 0
total_pop_value = 0
with arcpy.da.SearchCursor(pop_layer, ["grid_code"]) as cursor:
    for row in cursor:
        if row[0]:
            total_pop_value += row[0]

# 获取覆盖人口值
arcpy.management.SelectLayerByLocation(pop_layer, "WITHIN", buffer_layer, "", "NEW_SELECTION")
with arcpy.da.SearchCursor(pop_layer, ["grid_code"]) as cursor:
    for row in cursor:
        if row[0]:
            covered_pop_value += row[0]

# 计算研究区总缓冲区面积
area_sqkm = 0
with arcpy.da.SearchCursor(buffer_dissolved, ["SHAPE@AREA"]) as cursor:
    for row in cursor:
        area_sqkm += row[0]

# 获取研究区面积
study_area_sqkm = 0
with arcpy.da.SearchCursor(merged_area, ["SHAPE@AREA"]) as cursor:
    for row in cursor:
        study_area_sqkm += row[0]

coverage_rate = (covered_pop_value / total_pop_value * 100) if total_pop_value > 0 else 0

print(f"\n{'='*60}")
print(f"现有前置仓覆盖人口分析结果")
print(f"{'='*60}")
print(f"  前置仓数量: 51")
print(f"  服务半径: 3km")
print(f"  研究区面积: {study_area_sqkm/1e6:.2f} km2")
print(f"  缓冲区面积(合并后): {area_sqkm/1e6:.2f} km2")
print(f"  研究区内人口点数: {total_count_in_study}")
print(f"  研究区内总人口值: {total_pop_value:.2f}")
print(f"  覆盖人口点数: {covered_count}")
print(f"  覆盖人口值: {covered_pop_value:.2f}")
print(f"  覆盖率: {coverage_rate:.2f}%")

# 保存结果
result_table = os.path.join(out_gdb, "现有覆盖统计")
arcpy.management.CreateTable(out_gdb, "现有覆盖统计")
arcpy.management.AddField(result_table, "指标", "TEXT", field_length=50)
arcpy.management.AddField(result_table, "值", "DOUBLE")

with arcpy.da.InsertCursor(result_table, ["指标", "值"]) as cursor:
    cursor.insertRow(["前置仓数量", 51])
    cursor.insertRow(["服务半径_km", 3])
    cursor.insertRow(["研究区面积_km2", round(study_area_sqkm/1e6, 2)])
    cursor.insertRow(["缓冲区面积_km2", round(area_sqkm/1e6, 2)])
    cursor.insertRow(["研究区人口点数", total_count_in_study])
    cursor.insertRow(["研究区总人口", round(total_pop_value, 2)])
    cursor.insertRow(["覆盖人口点数", covered_count])
    cursor.insertRow(["覆盖人口", round(covered_pop_value, 2)])
    cursor.insertRow(["覆盖率_%", round(coverage_rate, 2)])

print(f"\n结果已保存到: {result_table}")
print("分析完成!")
