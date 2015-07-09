package com.dianping.data.warehouse.canaan.dolite;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.dianping.data.warehouse.canaan.common.Constants;
//import com.dianping.data.warehouse.halley.domain.InstanceDisplayDO;
//import com.dianping.data.warehouse.halley.service.InstanceService;
//import com.dianping.pigeon.remoting.ServiceFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;


import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VelocityDOLiteFactory implements DOLiteFactory {
    Logger logger = LoggerFactory.getLogger(VelocityDOLiteFactory.class);

    private String fileEncoding = "utf-8";
    private File DOLHome = new File("/");
    private Properties props = new Properties();
    private StrBuilder template;
    private String[] statementStrings;

    public Properties getProps() {
        return props;
    }

    @Inject
    public void setProps(@Named("props") Properties props) {
        this.props = props;
    }


    public DOLite produce(String fileName, String str)
            throws ParseErrorException, MethodInvocationException,
            ResourceNotFoundException, IOException {
        List<String> statements = this.loadTemplateFrom(str)
                .evaluateWithContext().createStatements();
        return new DOLiteImpl(fileName, statements);
    }

    protected List<String> createStatements() {
        List<String> statements = new ArrayList<String>(Arrays.asList(statementStrings));
        return statements;
    }

//    private List<String> addParameters(List<String> statements) {
//        InstanceDisplayDO instance = getInstance();
//        if (instance == null)
//            return statements;
//        statements = addInstanceInfo(statements, instance);
//        statements = addOOMStatements(statements, instance);
//        return statements;
//    }

//    /**
//     * 设置任务实例的相关信息
//     */
//    private List<String> addInstanceInfo(List<String> statements, InstanceDisplayDO instance) {
//        String TASKID_PARA = "set galaxy.taskId=" + instance.getTaskId();
//        String INSTANCEID_PARA = "set galaxy.instanceId=" + instance.getTaskStatusId();
//        String RUNNUMBER_PARA = "set galaxy.runNumber=" + instance.getRunNum();
//        statements.add(0, RUNNUMBER_PARA);
//        statements.add(0, INSTANCEID_PARA);
//        statements.add(0, TASKID_PARA);
//        return statements;
//    }

    /**
     * 加入OOM调整参数
     */
//    private List<String> addOOMStatements(List<String> statements, InstanceDisplayDO instance) {
//        if (instance.getRunNum() == null || instance.getRunNum() < 2)
//            return statements;
//        if (instance.getJobCode() == null)
//            return statements;
//        if (!needAdjustOOM(instance.getJobCode().toString().trim()))
//            return statements;
//        ArrayList<String> adjustList = new ArrayList<String>(Arrays.asList(Constants.OOM_PARA_ADJUST));
//        adjustList.addAll(statements);
//        logger.info("oom parameters added, " + adjustList.toString());
//        return adjustList;
//    }

    /**
     * 根据jobCode判断是否加入OOM调整参数
     */
    private boolean needAdjustOOM(String jobCode) {
        String codes = this.props.get(Constants.BATCH_COMMON_VARS.OOM_NOT_ADJUST_CODE.toString()).toString();
        if (codes == null)
            return true;
        String notAdjustOOMCodes[] = codes.split(",");
        for (String code : notAdjustOOMCodes) {
            if (jobCode.equals(code.trim()))
                return false;
        }
        return true;
    }

    /**
     * 获得任务实例信息
     */
//    private InstanceDisplayDO getInstance() {
//        Object instId = this.props.get(Constants.BATCH_COMMON_VARS.BATCH_INST_ID.toString());
//        if (instId == null) {
//            return null;
//        }
//        InstanceService instanceService = null; // 获取远程服务代理
//        InstanceDisplayDO instance = null;
//        try {
//            instanceService = ServiceFactory.getService(InstanceService.class, 5000);
//            instance = instanceService.getInstanceByInstanceId(instId.toString());
//        } catch (Exception e) {
//            logger.error("pigeon service fails: " + e.toString());
//            e.printStackTrace();
//            return null;
//        }
//        return instance;
//    }

    public String getFileEncoding() {
        return fileEncoding;
    }

    @Inject
    public void setFileEncoding(@Named("fileEncoding") String fileEncoding) {
        this.fileEncoding = fileEncoding;
    }

    public File getDOLHome() {
        return DOLHome;
    }

    @Inject
    public void setDOLHome(@Named("DOLHome") File DOLHome) {
        this.DOLHome = DOLHome;
    }

    protected Context getContextFromProperties() {
        Map<String, String> env = new HashMap<String, String>();
        for (Object key : this.props.keySet()) {
            env.put(key.toString(), props.get(key).toString());
        }
        Context context = new VelocityContext();
        context.put("env", env);
        DateTimeContext dtCtx = new DateTimeContext();
        context.put("dt", dtCtx);
        return context;

    }

    protected VelocityDOLiteFactory evaluateWithContext()
            throws ParseErrorException, MethodInvocationException,
            ResourceNotFoundException, IOException {
        StringWriter writer = new StringWriter();
        Velocity.addProperty("runtime.log", "");
        Velocity.addProperty("file.resource.loader.path",
                this.DOLHome.toString());
        Velocity.evaluate(getContextFromProperties(), writer, getClass()
                .getSimpleName(), this.template.asReader());
        //对sql中分号转义，参见hive -f 中处理方式
        ArrayList<String> statementArray = new ArrayList<String>();
        String command = "";
        String line = writer.toString();
        for (String oneCmd : line.split(";")) {
            if (StringUtils.endsWith(oneCmd, "\\")) {
                command += oneCmd + ";";
                continue;
            } else {
                command += oneCmd;
            }
            if (StringUtils.isBlank(command)) {
                continue;
            }
            statementArray.add(command);
            command = "";
        }

        int i = 0;
        this.statementStrings = new String[statementArray.size()];
        for (String statement : statementArray) {
            this.statementStrings[i] = statement.trim();
            i++;
        }

        return this;
    }

    protected VelocityDOLiteFactory loadTemplateFrom(String str)
            throws IOException {
        this.template = new StrBuilder(str);
        final byte[] bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
        if (this.template.toString().startsWith(new String(bom, "UTF8"))) {
            this.template.delete(0, 1);
        }
        String commonVmPath = "vm" + File.separator + "common.vm";
        if (new File(this.DOLHome + File.separator + commonVmPath).exists()) {
            String strParseVm = "#parse(\"" + commonVmPath + "\")";
            this.template.insert(0, strParseVm);
        }
        return this;
    }
}
