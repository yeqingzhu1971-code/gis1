# -*- coding: utf-8 -*-
"""
新增5个前置仓选址：
在现有51个前置仓基础上，以覆盖人口最大化为目标，
贪心算法选5个新增位置，对比前后覆盖差异
"""
import arcpy
import numpy as np
import os
import time

arcpy.env.overwriteOutput = True
arcpy.env.workspace = r"output.gdb"#补充完整路径

gdb = r"data.gdb"#补充完整路径
out_gdb = r"output.gdb"#补充完整路径
store_fc = os.path.join(gdb, "朴朴超市门店点")
merged_area = os.path.join(out_gdb, "研究区合并")
pop_points = os.path.join(gdb, "福州市人口栅格转点")
demand_pts = os.path.join(out_gdb, "需求点_带权重")
buffer_dissolved = os.path.join(out_gdb, "现有前置仓_3km缓冲区_合并")

print("=" * 60)
print("新增5个前置仓选址分析")
print("=" * 60)

# ========== 步骤1: 加载数据 ==========
print("\n步骤1: 加载数据...")

# 研究区内人口点
pop_layer = "pop_pts_layer"
arcpy.management.MakeFeatureLayer(pop_points, pop_layer)
arcpy.management.SelectLayerByLocation(pop_layer, "WITHIN", merged_area, "", "NEW_SELECTION")

pop_coords = []
pop_values = []
pop_oids = []
with arcpy.da.SearchCursor(pop_layer, ["OBJECTID", "SHAPE@XY", "grid_code"]) as cursor:
    for row in cursor:
        pop_oids.append(row[0])
        pop_coords.append(row[1])
        pop_values.append(row[2] if row[2] else 0)

pop_coords = np.array(pop_coords, dtype=np.float64)
pop_values = np.array(pop_values, dtype=np.float64)
n_pop = len(pop_coords)
total_pop = pop_values.sum()
print(f"研究区内人口点: {n_pop}, 总人口值: {total_pop:.2f}")

# ========== 步骤2: 计算现有前置仓已覆盖的人口点集合 ==========
print("\n步骤2: 计算现有前置仓已覆盖人口...")
arcpy.management.SelectLayerByLocation(pop_layer, "WITHIN", buffer_dissolved, "", "NEW_SELECTION")

existing_covered_oids = set()
existing_covered_value = 0
with arcpy.da.SearchCursor(pop_layer, ["OBJECTID", "grid_code"]) as cursor:
    for row in cursor:
        existing_covered_oids.add(row[0])
        existing_covered_value += row[1] if row[1] else 0

existing_cover_rate = existing_covered_value / total_pop * 100
uncovered_oids = set(range(n_pop)) - set(
    [i for i, oid in enumerate(pop_oids) if oid in existing_covered_oids]
)
# 重建: 将pop索引转为集合
existing_covered_indices = set()
for i, oid in enumerate(pop_oids):
    if oid in existing_covered_oids:
        existing_covered_indices.add(i)

print(f"现有覆盖人口: {existing_covered_value:.2f} ({existing_cover_rate:.2f}%)")
print(f"未覆盖人口点数: {n_pop - len(existing_covered_indices)}")

# ========== 步骤3: 生成候选点（排除现有覆盖区域内的点）==========
print("\n步骤3: 生成候选位置...")

# 从人口高密度且未被覆盖的区域采样
uncovered_indices = set(range(n_pop)) - existing_covered_indices
uncovered_pop_values = pop_values[list(uncovered_indices)]

# 未覆盖的人口值
uncovered_pop_total = uncovered_pop_values.sum()
print(f"未覆盖人口值: {uncovered_pop_total:.2f}")

# 候选点来源1: 需求点
demand_coords_list = []
with arcpy.da.SearchCursor(demand_pts, ["SHAPE@XY"]) as cursor:
    for row in cursor:
        demand_coords_list.append(row[0])
demand_coords = np.array(demand_coords_list, dtype=np.float64)

# 候选点来源2: 未覆盖区域中人口密度高的点
top_uncovered = sorted(uncovered_indices, key=lambda i: pop_values[i], reverse=True)[:500]
candidate_from_uncovered = pop_coords[top_uncovered]

# 合并候选点
all_candidates = np.vstack([demand_coords, candidate_from_uncovered])

# 去重
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
print(f"候选点数量: {len(candidates)}")

# ========== 步骤4: 预计算覆盖关系 ==========
print("\n步骤4: 预计算覆盖关系...")

BUFFER_DIST = 3000

# 只计算对未覆盖人口的覆盖关系（优化性能）
uncovered_pop_coords = pop_coords[list(uncovered_indices)]
uncovered_pop_values_arr = pop_values[list(uncovered_indices)]

# 分批计算
covered_uncovered = {}  # candidate_idx -> set of uncovered pop indices

batch_size = 50
for i in range(0, len(candidates), batch_size):
    end = min(i + batch_size, len(candidates))
    batch = candidates[i:end]
    dist_matrix = np.sqrt(((uncovered_pop_coords[np.newaxis, :, :] - batch[:, np.newaxis, :])**2).sum(axis=2))
    for j in range(end - i):
        covered_set = set(np.where(dist_matrix[j] <= BUFFER_DIST)[0])
        covered_uncovered[i + j] = covered_set
    if (i // batch_size) % 5 == 0:
        print(f"  进度: {end}/{len(candidates)}")

# 同时计算对所有人口点的覆盖关系（用于最终统计）
all_covered = {}
for i in range(0, len(candidates), batch_size):
    end = min(i + batch_size, len(candidates))
    batch = candidates[i:end]
    dist_matrix = np.sqrt(((pop_coords[np.newaxis, :, :] - batch[:, np.newaxis, :])**2).sum(axis=2))
    for j in range(end - i):
        all_covered[i + j] = set(np.where(dist_matrix[j] <= BUFFER_DIST)[0])

print(f"覆盖关系计算完成")

# ========== 步骤5: 贪心算法选择5个新增位置 ==========
print("\n步骤5: 贪心算法选择5个新增位置...")
start_time = time.time()

N_NEW = 5
new_selected = []
cumulative_covered = existing_covered_indices.copy()

for iteration in range(N_NEW):
    best_idx = -1
    best_gain = -1

    for cand_idx in covered_uncovered:
        # 新增覆盖 = 候选点覆盖的未覆盖人口
        new_covered_uncovered = covered_uncovered[cand_idx]
        # 计算这些点对应的人口值
        gain = uncovered_pop_values_arr[list(new_covered_uncovered)].sum() if new_covered_uncovered else 0
        if gain > best_gain:
            best_gain = gain
            best_idx = cand_idx

    if best_idx >= 0:
        new_selected.append(best_idx)
        # 更新已覆盖集合
        new_all_covered = all_covered[best_idx]
        cumulative_covered.update(new_all_covered)

        # 从uncovered中移除已被覆盖的
        newly_covered_from_uncovered = covered_uncovered[best_idx]
        # 更新未覆盖人口值（简单方式：不做精确更新，因为只选5个偏差不大）
        for uc_idx in newly_covered_from_uncovered:
            uncovered_pop_values_arr[uc_idx] = 0

    current_total_covered = pop_values[list(cumulative_covered)].sum()
    elapsed = time.time() - start_time
    print(f"  新增第{iteration+1}/{N_NEW}个: 候选点#{best_idx}, 新增覆盖={best_gain:.0f}, 累计覆盖={current_total_covered:.0f}, 覆盖率={current_total_covered/total_pop*100:.2f}%")

final_covered_pop = pop_values[list(cumulative_covered)].sum()
final_coverage_rate = final_covered_pop / total_pop * 100

print(f"\n新增选址完成!")
print(f"  新增后覆盖人口: {final_covered_pop:.2f}")
print(f"  新增后覆盖率: {final_coverage_rate:.2f}%")
print(f"  覆盖人口增量: {final_covered_pop - existing_covered_value:.2f}")

# ========== 步骤6: 保存新增选址结果 ==========
print("\n步骤6: 保存新增选址结果...")

spatial_ref = arcpy.Describe(demand_pts).spatialReference
new_stores_fc = os.path.join(out_gdb, "新增前置仓_5个")
arcpy.management.CreateFeatureclass(out_gdb, "新增前置仓_5个", "POINT", spatial_reference=spatial_ref)
arcpy.management.AddField(new_stores_fc, "StoreID", "LONG")
arcpy.management.AddField(new_stores_fc, "X", "DOUBLE")
arcpy.management.AddField(new_stores_fc, "Y", "DOUBLE")
arcpy.management.AddField(new_stores_fc, "NewCoveredPop", "DOUBLE")

# 计算每个新增仓的独立新增覆盖
cumulative = existing_covered_indices.copy()
for i, idx in enumerate(new_selected):
    new_covered = all_covered[idx] - cumulative
    new_pop = pop_values[list(new_covered)].sum()
    cumulative.update(all_covered[idx])
    with arcpy.da.InsertCursor(new_stores_fc, ["SHAPE@XY", "StoreID", "X", "Y", "NewCoveredPop"]) as cursor:
        cursor.insertRow([(candidates[idx][0], candidates[idx][1]), i + 1,
                         candidates[idx][0], candidates[idx][1], round(new_pop, 2)])

print(f"新增前置仓已保存: {new_stores_fc}")

# ========== 步骤7: 新增后总缓冲区 ==========
print("\n步骤7: 创建新增后总缓冲区...")

# 现有+新增门店合并（仅保留几何）
all_stores_fc = os.path.join(out_gdb, "AllStores_56")
# 先复制现有门店
arcpy.management.CopyFeatures(store_fc, all_stores_fc)
# 追加新增门店
arcpy.management.Append(new_stores_fc, all_stores_fc, "NO_TEST")

all_buffer = os.path.join(out_gdb, "AllStores_56_buffer")
arcpy.analysis.Buffer(all_stores_fc, all_buffer, "3 Kilometers", "FULL", "ROUND", "NONE", "", "PLANAR")

all_buffer_dissolved = os.path.join(out_gdb, "AllStores_56_buffer_dissolved")
arcpy.management.Dissolve(all_buffer, all_buffer_dissolved)

# 验证覆盖
arcpy.management.SelectLayerByLocation(pop_layer, "WITHIN", all_buffer_dissolved, "", "NEW_SELECTION")
verified_covered = 0
with arcpy.da.SearchCursor(pop_layer, ["grid_code"]) as cursor:
    for row in cursor:
        verified_covered += row[0] if row[0] else 0

verified_rate = verified_covered / total_pop * 100

# ========== 步骤8: 结果对比 ==========
print(f"\n{'='*60}")
print(f"新增5个前置仓前后对比")
print(f"{'='*60}")
print(f"  研究区总人口: {total_pop:.2f}")
print(f"  现有51个前置仓覆盖人口: {existing_covered_value:.2f} ({existing_cover_rate:.2f}%)")
print(f"  新增后56个前置仓覆盖人口: {final_covered_pop:.2f} ({final_coverage_rate:.2f}%)")
print(f"  GIS验证覆盖人口: {verified_covered:.2f} ({verified_rate:.2f}%)")
print(f"  覆盖人口增量: {final_covered_pop - existing_covered_value:.2f}")
print(f"  覆盖率提升: {final_coverage_rate - existing_cover_rate:.2f}%")

# 保存对比结果
result_table = os.path.join(out_gdb, "新增对比_5个")
arcpy.management.CreateTable(out_gdb, "新增对比_5个")
arcpy.management.AddField(result_table, "指标", "TEXT", field_length=50)
arcpy.management.AddField(result_table, "值", "DOUBLE")
with arcpy.da.InsertCursor(result_table, ["指标", "值"]) as cursor:
    cursor.insertRow(["原有前置仓数量", 51])
    cursor.insertRow(["新增前置仓数量", 5])
    cursor.insertRow(["合计前置仓数量", 56])
    cursor.insertRow(["研究区总人口", round(total_pop, 2)])
    cursor.insertRow(["原有覆盖人口", round(existing_covered_value, 2)])
    cursor.insertRow(["原有覆盖率", round(existing_cover_rate, 2)])
    cursor.insertRow(["新增后覆盖人口", round(final_covered_pop, 2)])
    cursor.insertRow(["新增后覆盖率", round(final_coverage_rate, 2)])
    cursor.insertRow(["GIS验证覆盖人口", round(verified_covered, 2)])
    cursor.insertRow(["GIS验证覆盖率", round(verified_rate, 2)])
    cursor.insertRow(["覆盖人口增量", round(final_covered_pop - existing_covered_value, 2)])
    cursor.insertRow(["覆盖率提升", round(final_coverage_rate - existing_cover_rate, 2)])

print(f"\n对比结果已保存: {result_table}")

# 输出新增仓坐标建议
print(f"\n{'='*60}")
print(f"P公司新增前置仓选址建议")
print(f"{'='*60}")
cumulative = existing_covered_indices.copy()
for i, idx in enumerate(new_selected):
    new_covered = all_covered[idx] - cumulative
    new_pop = pop_values[list(new_covered)].sum()
    cumulative.update(all_covered[idx])
    print(f"  建议位置{i+1}: X={candidates[idx][0]:.2f}, Y={candidates[idx][1]:.2f}, 新增覆盖人口={new_pop:.0f}")

print("\n分析完成!")
