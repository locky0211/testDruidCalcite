package com.tmp;

/**
 * 插入语句，select 表字段
 */
public abstract class SelectTableColumnTmpBase {


    public static final int TYPE_0 = 0;//未知类型
    public static final int TYPE_1 = 1;//直接表字段
    public static final int TYPE_2 = 2;//默认值
    public static final int TYPE_3 = 3;//额外处理
    public static final int TYPE_4 = 4;//UNION 多表数据合并


    public int type;
    public String desc;

    private String strs;


    public SelectTableColumnTmpBase(int type) {

        this.type = type;
        switch (type) {
            case TYPE_1:
                desc = "直接表字段";
                break;
            case TYPE_2:
                desc = "默认值";
                break;
            case TYPE_3:
                desc = "额外处理";
                break;
            default:
                desc = "未知类型";
                break;
        }
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getStrs() {
        return strs;
    }

    public void setStrs(String strs) {
        this.strs = strs;
    }
}
