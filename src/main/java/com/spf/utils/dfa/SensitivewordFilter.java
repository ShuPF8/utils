package com.spf.utils.dfa;

import java.util.*;

/**
 * @Description: 敏感词过滤
 * @Project：test
 * @version 1.0
 */
public class SensitivewordFilter {
	@SuppressWarnings("rawtypes")
	private static Map sensitiveWordMap = null;
	public static int minMatchTYpe = 1;      //最小匹配规则
	public static int maxMatchType = 2;      //最大匹配规则

	private static void initSensitiveWord(List<String> datas) {
		sensitiveWordMap = SensitiveWordInit.init(datas);
	}

	/**
	 * 替换敏感字字符
	 * @author chenming
	 * @date 2014年4月20日 下午5:12:07
	 * @param txt
	 * @param matchType
	 * @param replaceChar 替换字符，默认*
	 * @version 1.0
	 */
	public static String replaceSensitiveWord(List<String> datas, String txt,int matchType,String replaceChar){
		if (sensitiveWordMap == null) {
			initSensitiveWord(datas);
		}
		String resultTxt = txt;
		List<String> set = SensitiveWordInit.getSensitiveWord(txt, matchType);     //获取所有的敏感词
		System.out.println(set);
		Iterator<String> iterator = set.iterator();
		String word = null;
		String replaceString = null;
		while (iterator.hasNext()) {
			word = iterator.next();
			replaceString = getReplaceChars(replaceChar, word.length());
			resultTxt = resultTxt.replaceAll(word, replaceString);
		}
		return resultTxt;
	}

	/**
	 * 获取替换字符串
	 * @author chenming
	 * @date 2014年4月20日 下午5:21:19
	 * @param replaceChar
	 * @param length
	 * @return
	 * @version 1.0
	 */
	private static String getReplaceChars(String replaceChar,int length){
		String resultReplace = replaceChar;
		if (length > 6) {
			length = 6;
		}
		for(int i = 1 ; i < length ; i++){
			resultReplace += replaceChar;
		}
		return resultReplace;
	}


	/**
	 * 添加敏感词
	 * @param str
	 */
	private static boolean addSensitiveWord(String str) {
		if (sensitiveWordMap == null) {
			return false;
		}
		Map nowMap = null;
		Map<String, String> newWorMap = null;
			nowMap = sensitiveWordMap;
			for(int i = 0 ; i < str.length() ; i++){
				char keyChar = str.charAt(i);       //转换成char型
				Object wordMap = nowMap.get(keyChar);       //获取
				if(wordMap != null){        //如果存在该key，直接赋值
					nowMap = (Map) wordMap;
				}
				else{     //不存在则，则构建一个map，同时将isEnd设置为0，因为他不是最后一个
					newWorMap = new HashMap<String,String>();
					newWorMap.put("isEnd", "0");     //不是最后一个
					nowMap.put(keyChar, newWorMap);
					nowMap = newWorMap;
				}
				if(i == str.length() - 1){
					nowMap.put("isEnd", "1");    //最后一个
				}
			}
			return  true;
	}

	public static void main(String[] args) {
		List<String> datas = new ArrayList<String>();
        datas.add("滚");datas.add("卧槽");datas.add("你麻痹");datas.add("去你麻痹");
        datas.add("卧去");datas.add("卧去U");
        initSensitiveWord(datas);
        addSensitiveWord("MDZZQUN");
        String str = "卧去，尼玛，卧槽牛黄弩机滚,你麻痹你能见MDZZQUN到健康去你麻痹";
		String rep =  replaceSensitiveWord(datas,str,1,"*");
		System.out.println(rep);
	}
}
