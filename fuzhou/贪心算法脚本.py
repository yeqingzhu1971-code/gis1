# -*- coding: utf-8 -*-
"""
同数量前置仓优化选址：
以服务人口最大化为目标，用贪心算法从候选点中选51个最优位置
对比优化前后覆盖差异
"""
import arcpy
import numpy as np
import os
import time

arcpy.env.overwriteOutput = True
arcpy.env.workspace = r"output.gdb"#补充工作空间

gdb = r"data.gdb"#补充完整原始数据库
out_gdb = r"output.gdb"#补充完整输出数据库
merged_area = os.path.join(out_gdb, "研究区合并")
pop_points = os.path.join(gdb, "福州市人口栅格转点")
demand_pts = os.path.join(out_gdb, "需求点_带权重")
buffer_dissolved = os.path.join(out_gdb, "现有前置仓_3km缓冲区_合并")

print("=" * 60)
print("同数量前置仓优化选址 (51个)")
print("=" * 60)

# ========== 步骤1: 获取研究区内人口点及其坐标 ==========
print("\n步骤1: 加载研究区内人口点...")
pop_layer = "pop_pts_layer"
arcpy.management.MakeFeatureLayer(pop_points, pop_layer)
arcpy.management.SelectLayerByLocation(pop_layer, "WITHIN", merged_area, "", "NEW_SELECTION")

# 读取人口点坐标和人口值
pop_coords = []
pop_values = []
with arcpy.da.SearchCursor(pop_layer, ["SHAPE@XY", "grid_code"]) as cursor:
    for row in cursor:
        pop_coords.append(row[0])
        pop_values.append(row[0] if row[1] else 0)  # grid_code is index 1

# 修正：grid_code在第二个位置
pop_coords = []
pop_values = []
with arcpy.da.SearchCursor(pop_layer, ["SHAPE@XY", "grid_code"]) as cursor:
    for row in cursor:
        pop_coords.append(row[0])
        pop_values.append(row[1] if row[1] else 0)

pop_coords = np.array(pop_coords, dtype=np.float64)
pop_values = np.array(pop_values, dtype=np.float64)
n_pop = len(pop_coords)
print(f"研究区内人口点数: {n_pop}, 总人口值: {pop_values.sum():.2f}")

arcpy.management.SelectLayerByAttribute(pop_layer, "CLEAR_SELECTION")

# ========== 步骤2: 从人口点中生成候选点 ==========
print("\n步骤2: 生成候选位置...")

# 策略: 从需求点中筛选作为候选点，也可使用人口高密度区域
# 这里使用需求点(100个) + 额外从人口密集区域采样作为候选点

# 读取需求点
demand_coords = []
demand_weights = []
with arcpy.da.SearchCursor(demand_pts, ["SHAPE@XY", "Weight"]) as cursor:
    for row in cursor:
        demand_coords.append(row[0])
        demand_weights.append(row[1])

demand_coords = np.array(demand_coords, dtype=np.float64)

# 同时从高人口密度点中采样候选点
# 选取人口值top的点位
top_n = 500
top_indices = np.argsort(pop_values)[::-1][:top_n]
candidate_from_pop = pop_coords[top_indices]

# 合并候选点
all_candidates = np.vstack([demand_coords, candidate_from_pop])

# 去重（距离<500m的候选点合并）
from scipy.spatial.distance import cdist
keep_mask = np.ones(len(all_candidates), dtype=bool)
for i in range(len(all_candidates)):
    if not keep_mask[i]:
        continue
    dists = np.sqrt(np.sum((all_candidates - all_candidates[i])**2, axis=1))
    dists[i] = 999999
    close = np.where(dists < 500)[0]
    keep_mask[close] = False
    keep_mask[i] = True

candidates = all_candidates[keep_mask]
print(f"候选点数量: {len(candidates)} (需求点: {len(demand_coords)}, 人口高密度采样: {top_n})")

# ========== 步骤3: 预计算覆盖关系 ==========
print("\n步骤3: 预计算候选点与人口点的覆盖关系...")

BUFFER_DIST = 3000  # 地图单位

# 分批计算距离矩阵（内存优化）
print("计算距离矩阵...")
covered_pop = {}  # candidate_idx -> set of pop_point_indices

batch_size = 50
for i in range(0, len(candidates), batch_size):
    end = min(i + batch_size, len(candidates))
    batch = candidates[i:end]
    # 计算这批候选点到所有人口点的距离
    dist_matrix = np.sqrt(((pop_coords[np.newaxis, :, :] - batch[:, np.newaxis, :])**2).sum(axis=2))
    for j in range(end - i):
        covered = set(np.where(dist_matrix[j] <= BUFFER_DIST)[0])
        covered_pop[i + j] = covered
    if (i // batch_size) % 5 == 0:
        print(f"  进度: {end}/{len(candidates)}")

print(f"覆盖关系计算完成, 共{len(covered_pop)}个候选点")

# ========== 步骤4: 贪心算法选择51个最优位置 ==========
print("\n步骤4: 贪心算法优化选址 (N=51)...")
start_time = time.time()

N_STORES = 51
selected = []
covered_set = set()

for iteration in range(N_STORES):
    best_idx = -1
    best_gain = -1
    best_new_covered = set()

    for cand_idx, cand_covered in covered_pop.items():
        if cand_idx in selected:
            continue
        new_covered = cand_covered - covered_set
        # 计算新增覆盖的人口值
        gain = pop_values[list(new_covered)].sum() if new_covered else 0
        if gain > best_gain:
            best_gain = gain
            best_idx = cand_idx
            best_new_covered = new_covered

    if best_idx >= 0:
        selected.append(best_idx)
        covered_set.update(best_new_covered)

    elapsed = time.time() - start_time
    print(f"  第{iteration+1}/{N_STORES}个: 候选点#{best_idx}, 新增覆盖人口={best_gain:.0f}, 累计覆盖={pop_values[list(covered_set)].sum():.0f}, 耗时={elapsed:.1f}s")

total_covered_pop_optimized = pop_values[list(covered_set)].sum()
total_pop = pop_values.sum()
coverage_rate_opt = total_covered_pop_optimized / total_pop * 100

print(f"\n优化选址完成!")
print(f"  优化后覆盖人口: {total_covered_pop_optimized:.2f}")
print(f"  优化后覆盖率: {coverage_rate_opt:.2f}%")

# ========== 步骤5: 保存优化选址结果 ==========
print("\n步骤5: 保存优化选址结果...")

spatial_ref = arcpy.Describe(demand_pts).spatialReference
optimized_fc = os.path.join(out_gdb, "优化选址_51个")
arcpy.management.CreateFeatureclass(out_gdb, "优化选址_51个", "POINT", spatial_reference=spatial_ref)
arcpy.management.AddField(optimized_fc, "StoreID", "LONG")
arcpy.management.AddField(optimized_fc, "X", "DOUBLE")
arcpy.management.AddField(optimized_fc, "Y", "DOUBLE")

with arcpy.da.InsertCursor(optimized_fc, ["SHAPE@XY", "StoreID", "X", "Y"]) as cursor:
    for i, idx in enumerate(selected):
        cursor.insertRow([(candidates[idx][0], candidates[idx][1]), i + 1,
                         candidates[idx][0], candidates[idx][1]])

print(f"优化选址已保存: {optimized_fc}")

# ========== 步骤6: 优化选址缓冲区 ==========
print("\n步骤6: 创建优化选址缓冲区...")
optimized_buffer = os.path.join(out_gdb, "优化选址_51个_3km缓冲区")
arcpy.analysis.Buffer(optimized_fc, optimized_buffer, "3 Kilometers", "FULL", "ROUND", "NONE", "", "PLANAR")

optimized_buffer_dissolved = os.path.join(out_gdb, "优化选址_51个_缓冲区_合并")
arcpy.management.Dissolve(optimized_buffer, optimized_buffer_dissolved)

# ========== 步骤7: 计算现有覆盖（用于对比） ==========
print("\n步骤7: 计算现有前置仓覆盖人口（用于对比）...")
arcpy.management.SelectLayerByLocation(pop_layer, "WITHIN", buffer_dissolved, "", "NEW_SELECTION")
current_covered_value = 0
current_covered_set = set()
with arcpy.da.SearchCursor(pop_layer, ["OBJECTID", "grid_code"]) as cursor:
    for row in cursor:
        current_covered_value += row[1] if row[1] else 0
current_cover_rate = current_covered_value / total_pop * 100

# ========== 步骤8: 结果对比 ==========
print(f"\n{'='*60}")
print(f"优化前后覆盖对比 (均51个前置仓)")
print(f"{'='*60}")
print(f"  研究区总人口: {total_pop:.2f}")
print(f"  现有前置仓覆盖人口: {current_covered_value:.2f}")
print(f"  现有前置仓覆盖率: {current_cover_rate:.2f}%")
print(f"  优化选址覆盖人口: {total_covered_pop_optimized:.2f}")
print(f"  优化选址覆盖率: {coverage_rate_opt:.2f}%")
print(f"  覆盖人口增量: {total_covered_pop_optimized - current_covered_value:.2f}")
print(f"  覆盖率提升: {coverage_rate_opt - current_cover_rate:.2f}%")

# 保存对比结果
result_table = os.path.join(out_gdb, "优化对比_同数量")
arcpy.management.CreateTable(out_gdb, "优化对比_同数量")
arcpy.management.AddField(result_table, "指标", "TEXT", field_length=50)
arcpy.management.AddField(result_table, "值", "DOUBLE")
with arcpy.da.InsertCursor(result_table, ["指标", "值"]) as cursor:
    cursor.insertRow(["前置仓数量", 51])
    cursor.insertRow(["研究区总人口", round(total_pop, 2)])
    cursor.insertRow(["现有覆盖人口", round(current_covered_value, 2)])
    cursor.insertRow(["现有覆盖率", round(current_cover_rate, 2)])
    cursor.insertRow(["优化覆盖人口", round(total_covered_pop_optimized, 2)])
    cursor.insertRow(["优化覆盖率", round(coverage_rate_opt, 2)])
    cursor.insertRow(["覆盖人口增量", round(total_covered_pop_optimized - current_covered_value, 2)])
    cursor.insertRow(["覆盖率提升", round(coverage_rate_opt - current_cover_rate, 2)])

print(f"\n对比结果已保存: {result_table}")
print("分析完成!")
