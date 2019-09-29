package com.wp.redis.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 对象转换
 * Created by wangpeng on 2018/4/24.
 */
public class ObjectUtil {

    private static Logger log = LoggerFactory.getLogger(ObjectUtil.class);

    /**
     * objectid包路径
     */
    private final static String OBJECTID_PACKAGE = "org.bson.types.ObjectId";

    /**
     * booktype包路径
     */
    private final static String BOOK_TYPE_PACKAGE = "net.iyouqu.union.enums.BookType";

    private final static String STATUS = "net.iyouqu.union.enums.Status";

    private final static String TERMINAL = "net.iyouqu.union.enums.Terminal";
    /**
     * 日期包路径
     */
    private final static String DATE_PACKAGE = "java.util.Date";
    /**
     * float
     */
    private final static String FLOAT = "float";

    /**
     * 字符串数组
     */
    private final static String STRING_ARRAY = "[Ljava.lang.String;";

    /**
     * 将一个 JavaBean 对象转化为一个  Map
     *
     * @param bean 要转化的JavaBean 对象
     * @return 转化出来的  Map 对象
     * @throws IntrospectionException    如果分析类属性失败
     * @throws IllegalArgumentException
     * @throws IllegalAccessException    如果实例化 JavaBean 失败
     * @throws InvocationTargetException 如果调用属性的 setter 方法失败
     */
    public static Map<String, Object> convertBean(Object bean) throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Class type = bean.getClass();

        BeanInfo beanInfo = Introspector.getBeanInfo(type);

        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        Map<String, Object> returnMap = new HashMap<>(propertyDescriptors.length);
        for (int i = 0; i < propertyDescriptors.length; i++) {
            PropertyDescriptor descriptor = propertyDescriptors[i];
            String propertyName = descriptor.getName();
            if (!propertyName.equals("class")) {
                Method readMethod = descriptor.getReadMethod();
                Object result = readMethod.invoke(bean);
                if (result != null) {
                    if (result instanceof Date) {
                        result = ((Date) result).getTime();
                    }
                    if (result instanceof ObjectId) {
                        result = result.toString();
                    }
                    if (result instanceof String[]) {
                        result = JSONObject.toJSONString(result);
                    }
                    returnMap.put(propertyName, result);
                }
            }
        }
        return returnMap;
    }


    /**
     * 将一个 Map 对象转化为一个 JavaBean
     *
     * @param type 要转化的类型
     * @param map  包含属性值的 map
     * @return 转化出来的 JavaBean 对象
     * @throws IntrospectionException    如果分析类属性失败
     * @throws IllegalAccessException    如果实例化 JavaBean 失败
     * @throws InstantiationException    如果实例化 JavaBean 失败
     * @throws InvocationTargetException 如果调用属性的 setter 方法失败
     */

    public static Object convertMap(Class type, Map map)
            throws IntrospectionException, IllegalAccessException,
            InstantiationException, NoSuchMethodException, InvocationTargetException {
        // 获取类属性
        BeanInfo beanInfo = Introspector.getBeanInfo(type);
        // 创建 JavaBean 对象
        Object obj = type.newInstance();

        // 给 JavaBean 对象的属性赋值
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (int i = 0; i < propertyDescriptors.length; i++) {
            PropertyDescriptor descriptor = propertyDescriptors[i];
            String propertyName = descriptor.getName();

            if (map.containsKey(propertyName)) {
                Class<?> propertyType = descriptor.getPropertyType();
                Object value = map.get(propertyName);
                if (propertyType.isEnum()) {
                    Method method = propertyType.getMethod("valueOf", String.class);
                    value = method.invoke(null, value.toString());
                } else if (propertyType.isArray()) {
                    value = JSONObject.parseArray(value.toString()).toArray(new String[0]);
                } else {
                    value = convertValType(value, propertyType);
                }
                try {
                    descriptor.getWriteMethod().invoke(obj, value);
                } catch (InvocationTargetException e) {
                    log.info("缺少set方法", propertyName);
                }
            }
        }
        return obj;
    }

    private static Object convertValType(Object value, Class<?> fieldTypeClass) {
        Object retVal;
        if (StringUtils.isBlank(value.toString())) {
            if (!String.class.getName().equals(fieldTypeClass.getName())) {
                value = 0;
            }
        }
        if (Long.class.getName().equals(fieldTypeClass.getName()) || long.class.getName().equals(fieldTypeClass.getName())) {
            retVal = Long.parseLong(value.toString());
        } else if (Integer.class.getName().equals(fieldTypeClass.getName()) || int.class.getName().equals(fieldTypeClass.getName())) {
            retVal = Integer.parseInt(value.toString());
        } else if (Float.class.getName().equals(fieldTypeClass.getName()) || float.class.getName().equals(fieldTypeClass.getName())) {
            retVal = Float.parseFloat(value.toString());
        } else if (Double.class.getName().equals(fieldTypeClass.getName()) || double.class.getName().equals(fieldTypeClass.getName())) {
            retVal = Double.parseDouble(value.toString());
        } else if (ObjectId.class.getName().equals(fieldTypeClass.getName())) {
            retVal = new ObjectId(value.toString());
        } else if (Date.class.getName().equals(fieldTypeClass.getName())) {
            retVal = new Date((long) value);
        } else {
            retVal = value;
        }
        return retVal;
    }
}
