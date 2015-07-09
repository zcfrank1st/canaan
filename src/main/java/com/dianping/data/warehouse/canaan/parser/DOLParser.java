package com.dianping.data.warehouse.canaan.parser;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.dolite.VelocityDOLiteFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;

import com.dianping.data.warehouse.canaan.dolite.DOLite;
import com.dianping.data.warehouse.canaan.dolite.DOLiteFactory;
//import com.dianping.data.warehouse.canaan.dolite.VelocityDOLiteFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.hsqldb.lib.StringUtil;

public class DOLParser extends AbstractModule {
	private DOLite dolite;
	private String DOLHome;
	private Properties props;
	private String fileName;
    private String str;
	private String taskId;
    private static final String fileEncoding = "utf-8";
	@Override
	protected void configure() {
		bind(DOLiteFactory.class).to(VelocityDOLiteFactory.class);
		bind(File.class).annotatedWith(Names.named("DOLHome")).toInstance(new File(this.DOLHome));

		bind(String.class).annotatedWith(Names.named("fileEncoding")).toInstance(fileEncoding);

		bind(Properties.class).annotatedWith(Names.named("props")).toInstance(this.props);
	}

	public DOLParser(CanaanConf canaanConf) throws IOException {
		//modify by hongdi
		String product = canaanConf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_PRODUCT.toString());
		
		String group = canaanConf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_GROUP.toString());
		String baseHome = canaanConf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_BASE_DOL_DIR.toString());
		
		System.err.println("product :="+ product);
		System.err.println("group :="+ group);
		System.err.println("baseHome :="+ baseHome);
		
		if(!StringUtil.isEmpty(group)){
			if(!StringUtil.isEmpty(product)){
				this.DOLHome = new StringBuilder().append(baseHome).append(File.separator).append(group).append(File.separator).append(product).toString();
			}else{
				this.DOLHome = new StringBuilder().append(baseHome).append(File.separator).append(group).toString();
			}
								
		}else{
			this.DOLHome = canaanConf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_DOL_DIR.toString());
		}
		
		System.err.println("DOLHome :="+ DOLHome);
		
        this.fileName = canaanConf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_DOL.toString());
        if (canaanConf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_DOL_TYPE.toString()).equals(Constants.DOL_TYPE_DOL))
        {
            String fullPath = FilenameUtils
                    .concat(this.DOLHome,this.fileName);   
            this.str = FileUtils.readFileToString(new File(fullPath), fileEncoding);
        }
        else
        {
            this.str = canaanConf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_DOL_STR.toString());
        }
		this.taskId = canaanConf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_TASK_ID.toString());
		this.props = canaanConf.getCanaanProperties();
	}

	public DOLite getDOLite() throws Exception {
		Injector injector = Guice.createInjector(new Module[] { this });
		DOLiteFactory factory = (DOLiteFactory) injector.getInstance(DOLiteFactory.class);
		dolite = factory.produce(fileName,str);
		return dolite;
	}
}

