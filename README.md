# druidhll
python version of druid hll 

druid 使用的hll算法

druid origin hll 精度和宽度固定 11，4. 可以在导入时使用spark提前计算好hll。相比使用dr的 roll up 功能更加高效

导入 metric 定义：
    {
      "type": "hyperUnique",
      "name": "hll_uv",
      "fieldName": "hll_uv",
      "isInputHyperUnique": true,
      "round": false
    }
