package utils;

import java.lang.reflect.Field;

import configuration.ConfigBean;

public class ReflectUtils {
	public static Field getFieldByFiledName(String fieldName) {
		try {
			return ConfigBean.class.getDeclaredField(fieldName);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void setFieldValue(Field field, ConfigBean bean, String value) {
		try {
			field.setAccessible(true);
			field.set(bean, value);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	public static String getFieldValue(Field field) {
		Object instance = instanceFiled(field);
		try {
			return (String) field.get(instance);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Object instanceFiled(Field field) {
		Class<?> targetType = field.getType();
		field.setAccessible(true);
		try {
			return targetType.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
}
