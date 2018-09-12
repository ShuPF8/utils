//package com.spf.utils.generator;
//
//import org.mybatis.generator.api.MyBatisGenerator;
//import org.mybatis.generator.api.ProgressCallback;
//import org.mybatis.generator.api.VerboseProgressCallback;
//import org.mybatis.generator.config.Configuration;
//import org.mybatis.generator.config.xml.ConfigurationParser;
//import org.mybatis.generator.internal.DefaultShellCallback;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @author ShuPF
// * @类说明：
// * @date 2018-09-10 11:31
// */
//public class GeneratorUtils {
//
//    public static void main(String[] args) {
//        shell();
//    }
//
//    private static void shell() {
//        List<String> warnings = new ArrayList<String>();
//        try {
//            String configFilePath = System.getProperty("user.dir").concat("/src/main/resources/generatorConfig.xml");
//            System.out.println("加载配置文件===" + configFilePath);
//            boolean overwrite = true;
//            File configFile = new File(configFilePath);
//            ConfigurationParser cp = new ConfigurationParser(warnings);
//            Configuration config = cp.parseConfiguration(configFile);
//            DefaultShellCallback callback = new DefaultShellCallback(overwrite);
//            MyBatisGenerator myBatisGenerator = new MyBatisGenerator(config, callback, warnings);
//            ProgressCallback progressCallback = new VerboseProgressCallback();
//            // myBatisGenerator.generate(null);
//            myBatisGenerator.generate(progressCallback);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
//        for (String warning : warnings) {
//            System.out.println(warning);
//        }
//    }
//
//}
