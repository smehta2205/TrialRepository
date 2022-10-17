/**
 * Copyright (C) 2011 OpsHub, Inc. All rights reserved
 */
package com.opshub.eai.business;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import com.google.common.base.Joiner;
import com.opshub.Constants.AdminConstants;
import com.opshub.Constants.Constants.BGPJobConstants;
import com.opshub.aaa.authentication.UserInformation;
import com.opshub.business.admin.AdminUIConstants;
import com.opshub.business.admin.BaseBO;
import com.opshub.business.admin.JobsBO;
import com.opshub.business.admin.KeyValue;
import com.opshub.business.admin.ListItem;
import com.opshub.client.datatransfer.eai.BidirectionalIntegrationInfo;
import com.opshub.client.datatransfer.eai.ConfigDirection;
import com.opshub.client.datatransfer.eai.ConfigMode;
import com.opshub.client.datatransfer.eai.ContextData;
import com.opshub.client.datatransfer.eai.CriteriaContext;
import com.opshub.client.datatransfer.eai.CriteriaInfo;
import com.opshub.client.datatransfer.eai.EAIIntegrationInfo;
import com.opshub.client.datatransfer.eai.EAIProcessDefinfo;
import com.opshub.client.datatransfer.eai.EAISourceEventInfo;
import com.opshub.client.datatransfer.eai.IntegrationGroupContext;
import com.opshub.client.datatransfer.eai.IntegrationGroupInfo;
import com.opshub.client.datatransfer.eai.IntegrationObjectformatConverter;
import com.opshub.client.datatransfer.eai.LicenseEndpointVerificationData;
import com.opshub.client.datatransfer.eai.ProjectMappingContext;
import com.opshub.client.datatransfer.eai.RemoteLinkage;
import com.opshub.client.datatransfer.eai.TargetLookup;
import com.opshub.client.datatransfer.eai.UnidirectionalIntegrationData;
import com.opshub.client.datatransfer.eai.reconcile.EaiWorkflow;
import com.opshub.client.datatransfer.eai.reconcile.ReconcileValidatorResponse;
import com.opshub.client.datatransfer.eai.reconcile.ReconciliationInfo;
import com.opshub.client.datatransfer.eai.reconcile.ResponseValidatiorObject;
import com.opshub.customconfig.CustomConfiguration;
import com.opshub.customconfig.CustomConfigurationFactory;
import com.opshub.customconfig.OIMConfiguration;
import com.opshub.dao.core.Credentials;
import com.opshub.dao.core.EAIGroupSyncProgress;
import com.opshub.dao.core.EAIProcessedUser;
import com.opshub.dao.core.EaiEventProcessDefinition;
import com.opshub.dao.core.EaiEventTransformationMapping;
import com.opshub.dao.core.EaiEventsLogged;
import com.opshub.dao.core.EaiProcessDefinition;
import com.opshub.dao.core.Entities;
import com.opshub.dao.core.EventTime;
import com.opshub.dao.core.Job;
import com.opshub.dao.core.JobContext;
import com.opshub.dao.core.JobGroup;
import com.opshub.dao.core.JobInstance;
import com.opshub.dao.core.JobSchedule;
import com.opshub.dao.core.JobStatus;
import com.opshub.dao.core.OHProduct;
import com.opshub.dao.core.Scripts;
import com.opshub.dao.core.SourceFailed;
import com.opshub.dao.core.SourceSystem;
import com.opshub.dao.core.SystemEntityMapping;
import com.opshub.dao.core.SystemExtension;
import com.opshub.dao.core.SystemType;
import com.opshub.dao.core.Systems;
import com.opshub.dao.core.Types;
import com.opshub.dao.core.ViewTypes;
import com.opshub.dao.eai.EAICommentPolledState;
import com.opshub.dao.eai.EAICommentPolledState_;
import com.opshub.dao.eai.EAICriteria;
import com.opshub.dao.eai.EAIEventsSkipped;
import com.opshub.dao.eai.EAIIntegrationGroup;
import com.opshub.dao.eai.EAIIntegrationStatus;
import com.opshub.dao.eai.EAIIntegrations;
import com.opshub.dao.eai.EAIIntegrationsBase;
import com.opshub.dao.eai.EAIIntegrations_;
import com.opshub.dao.eai.EAIJobMappings;
import com.opshub.dao.eai.EAIMapperIntermidiateXML;
import com.opshub.dao.eai.EAIPollingType;
import com.opshub.dao.eai.EAIProcessContext;
import com.opshub.dao.eai.EAIProjectMappingBase;
import com.opshub.dao.eai.EAIProjectMappings;
import com.opshub.dao.eai.EAIReconciliations;
import com.opshub.dao.eai.EAISystemEvents;
import com.opshub.dao.eai.EAITimeboxEntityHistoryState;
import com.opshub.dao.eai.EaiEventType;
import com.opshub.dao.eai.EaiNotificationIntegration;
import com.opshub.dao.eai.EaiNotificationParams;
import com.opshub.dao.eai.EntityLastUpdateTimeInfo;
import com.opshub.dao.eai.EntityLastUpdateTimeInfo_;
import com.opshub.dao.eai.OIMEntityContext;
import com.opshub.dao.eai.OIMEntityCreationLock;
import com.opshub.dao.eai.OIMEntityCreationLock_;
import com.opshub.dao.eai.OIMEntityHierarchyInfo;
import com.opshub.dao.eai.OIMEntityHistoryState;
import com.opshub.dao.eai.OIMEntityInfo;
import com.opshub.dao.eai.OIMEntitySyncInfo;
import com.opshub.dao.eai.OIMEntitySyncInfo_;
import com.opshub.dao.eai.ReconcileContext;
import com.opshub.eai.Constants;
import com.opshub.eai.Constants.BidirectionalValidationConstants;
import com.opshub.eai.Constants.DeleteJobPollingType;
import com.opshub.eai.EAIJobConstants;
import com.opshub.eai.EAIKeyValue;
import com.opshub.eai.EAISystemExtensionConstants;
import com.opshub.eai.EaiExtParamLoader;
import com.opshub.eai.EaiUtility;
import com.opshub.eai.EndSystemFields;
import com.opshub.eai.core.adapters.AdapterLoader;
import com.opshub.eai.core.adapters.caching.IOIMCacheableAdapter;
import com.opshub.eai.core.exceptions.ConnectorLoaderException;
import com.opshub.eai.core.exceptions.OIMAdapterException;
import com.opshub.eai.core.interfaces.IOIMConnector;
import com.opshub.eai.core.utility.ConnectorLoaderFactory;
import com.opshub.eai.core.utility.CriteriaHandler;
import com.opshub.eai.core.utility.IConnectorLoader;
import com.opshub.eai.core.utility.ProcessContextInterface;
import com.opshub.eai.edition.common.EditionConstants;
import com.opshub.eai.edition.common.EditionConstants.Features;
import com.opshub.eai.edition.data.EditionDetailsLoader;
import com.opshub.eai.edition.data.FreeEditionGlobalScheduleLimitInitializer;
import com.opshub.eai.edition.exception.OIMEditionException;
import com.opshub.eai.edition.validation.EntityTypeValidator;
import com.opshub.eai.jira.common.JiraUtility;
import com.opshub.eai.jira.exceptions.OIMJiraApiException;
import com.opshub.eai.mapper.MapperUtility;
import com.opshub.eai.mapper.server.MapperBO;
import com.opshub.eai.mapper.server.OIMMapperException;
import com.opshub.eai.metadata.DataType;
import com.opshub.eai.metadata.EntityMeta;
import com.opshub.eai.metadata.FieldsMeta;
import com.opshub.eai.metadata.MetadataException;
import com.opshub.eai.metadata.MetadataImplFactory;
import com.opshub.eai.metadata.MetadataImplFactoryException;
import com.opshub.eai.metadata.ProjectMeta;
import com.opshub.eai.metadata.interfaces.HasMetadata;
import com.opshub.eai.pipeline.exceptions.PipelineException;
import com.opshub.eai.pipeline.json.PipelineJsonToWorkflow;
import com.opshub.eai.ptc.common.PTCConstants;
import com.opshub.eai.smartbear.common.OIMSmartBearBO;
import com.opshub.eai.test.AutomationDataHandler;
import com.opshub.eai.test.TestCaseDataCarrier;
import com.opshub.eai.tfs.git.TFSGitConstants;
import com.opshub.eai.tfs.querydashboard.TFSQueryDashboardConstants;
import com.opshub.exceptions.DataValidationException;
import com.opshub.exceptions.EncryptionAndDecryptionException;
import com.opshub.exceptions.FormLoaderException;
import com.opshub.exceptions.IntegrationValidationResult;
import com.opshub.exceptions.InvalidConfiguration;
import com.opshub.exceptions.LoggerManagerException;
import com.opshub.exceptions.MetadataNotFoundException;
import com.opshub.exceptions.ORMException;
import com.opshub.exceptions.OpsHubBaseException;
import com.opshub.exceptions.OpsInternalError;
import com.opshub.exceptions.ScheduleCreationException;
import com.opshub.exceptions.eai.EAIActionHandlerException;
import com.opshub.exceptions.eai.EAIProcessException;
import com.opshub.jobs.core.JobFailureAnalyzer;
import com.opshub.jobs.core.JobSchedular;
import com.opshub.jobs.core.JobSchedulerThread;
import com.opshub.license.exception.LicenseException;
import com.opshub.license.exception.MigrationLicenseException;
import com.opshub.license.verify.IntegrationTargetPointInfo;
import com.opshub.license.verify.LicenseEditionVerifier;
import com.opshub.license.verify.LicenseVerifier;
import com.opshub.license.verify.ReconcilationWithMigrationLicenseVerifier;
import com.opshub.logging.OpsHubLoggingUtil;
import com.opshub.server.LoggerManager.LogFileLocationPath;
import com.opshub.server.LoggerManager.LoggerHandler;
import com.opshub.server.LoggerManager.LoggerManager;
import com.opshub.server.admin.Action;
import com.opshub.server.admin.EntityViewTypeInfo;
import com.opshub.server.admin.FieldValue;
import com.opshub.server.admin.FormLoader;
import com.opshub.server.admin.UIConstants;
import com.opshub.server.admin.integrationconfiguration.IntegrationConfiguration;
import com.opshub.server.admin.integrationconfiguration.IntegrationStatusInfo;
import com.opshub.server.admin.mapperconfiguration.MetaDataItem;
import com.opshub.utils.DateUtils;
import com.opshub.utils.EncryptionAndDecryptionUtility;
import com.opshub.utils.OHProductInformationUtil;
import com.opshub.utils.Util;
import com.opshub.utils.hibernate.DbOperationsUtil;
import com.opshub.utils.hibernate.HibernateSessionFactory;

import lombok.Getter;
import lombok.Setter;

public class EaiConfigBO extends BaseBO {

	private static final String ACTIVE = "Active";
	private static final Map<String, Integer> CACHED_VIEW_TYPES = new HashMap<>();
	private static final Map<String, List<SystemEntityMappingCacheInfo>> CACHED_SYSTEM_ENTITY_MAPPING = new HashMap<>();
	private static final Map<Integer, String> CACHED_POLLING_TYPE = new HashMap<>();
	private static final String _0022 = "0022";
	private static final String AND_OH_PRODUCT_OH_PRODUCT_ID = " and ohProduct.ohProductId=";
	private static final String AND_TYPE_TYPE_ID = " and type.typeId = ";
	private static final String WHERE_SYSTEM_TYPE_SYSTEM_TYPE_ID = " where systemType.systemTypeId =:";
	public static final String IN_ACTIVE = "InActive";
	public static final String EXECUTE = "Execute";
	private static final String KEY = "key";
	private static final String DUMMY_CREATE_UPDATE_INTEGRATION = "DUMMY_CREATE_UPDATE_INTEGRATION";
	private static final String _017652 = "017652";
	private static final String WHERE_BASE_INTEGRATION_INTEGRATION_BASE_ID = " where baseIntegration.integrationBaseId=";
	public static final String RUN = "Run";
	private static final String VALUE = "value";
	private static final String FROM = "from ";
	private enum JobType { RECO, INTEGRATION, DELETE}
	private static final String INTEGRATION = "integration";
	private static final String INTEGRATION_POLLING_TIME = "integrationPollingTime";
	private static final String USERT_SET_POLLING_TIME = "usertSetPollingTime";
	private static final String NAME = "name";
	private static final String SYSTEM_ID = "systemId";
	private static final String INTEGRATION_ID = "integrationId";
	private static final String JOB_INSTANCE = "jobInstance";
	private static final String STATUS = "status";
	private static final String FAILED_EVENTS_COUNT = "failedEventsCount";
	private static final String ENDPOINT_1_ISSUETYPE = "ep1IssueType";
	private static final String ENDPOINT_2_ISSUETYPE = "ep2IssueType";
	private static final String LOG = ".log";

	private final static Logger LOGGER = Logger.getLogger(EaiConfigBO.class);

	private static final String PARAM_JOB_INSTANCE = "jobInstanceId";
	private static final String KEY_CONFIGURE_CRITERIA = "configureCriteria";
	private static final String KEY_QUERY = "query";
	private static final String SYSTEM_ID_QUERY = "select baseIntegration.integrationGroup.endPoint1.systemType.parentSystemType.systemTypeId,baseIntegration.integrationGroup.endPoint2.systemType.parentSystemType.systemTypeId FROM "
			+ EAIIntegrations.class.getName() + " WHERE integrationId =:integrationId";
	private static final String WHERE_INTEGRATION_ID = " where integration.integrationId =:id";
	private static final String SWITCHING_TO_INTEGRATION_MODE = "Switching to Integration mode";

	public interface ReconcileValidation {
		String ERROR_MSG_FOR_GROUP_STATUS_CHANGE_WITH_EXPIRED_RECO = "Status can not changed for the reconcile(s) as reconcile execution completed, "
				+ "So either first switched to integration mode or enable the reconcile again.";
		String ERROR_MSG_RECONCILE_RULE_NOT_CONFIGURED = "Reconcile rules have not been configured on it's field mapping";
		String ERROR_MSG_FOR_RECONCILE_INTEGRATION_ACTIVE = "Integration is in Active state";
		String ERROR_MSG_FOR_RECONCILE_LICENSE = "OIM Edition is downgraded,Reconcile can not be activated.";
		String SUCCESSFULL_TRANSTION = "transition successful to ";
		String RECONCILE = "Reconciliation";
		String EXECUTE_STATUS_NOT_ALLOWED = "Execute or Execute All status is not applicable to reconciliation.";
		String RECONCILE_ACTIVE_STATUS_NOT_ALLOWED_WITHOUT_MIGRATION_LIC = "Reconciliation for integration having criteria can not be activated in bulk as you don't have migration license. You can activate them one by one.";

	}

	public interface IntegrationValidation {
		String INVALID_TRANSIOTION = "Invalid status transition";
		String JOB_NOT_SCHEDULED_CORRECTLY = "Integration job not scheduled correctly, Verify whether any error logged as integration log or job error";
		String STATUS_CHANGED_SUCCESSFULL = "Status changed successfully";
		String INTEGRATION = "Integration";
	}

	private static final List<String> SRC_JOB_CTXT_SKIP_LIST = Arrays.asList(Constants.IS_TARGET_LOOKUP,Constants.TARGET_LOOKUP_QUERY,Constants.RESOLUTION_OPTION,Constants.IS_CONTINUE_UPDATE,Constants.IS_CREATE_ON_NOT_FOUND, Constants.DELETE_JOB_SCHEDULE);
	// Smart Bear locking specific
	private static final String LOCK = "lock";
	private static final String SB_LOCKED = "1";

	private String jiraWorkFlowFileName;

	private static final Map<String, String> PROJECT_KEY_WITH_DISPLAY_NAME = new HashMap<>();
	private static final int PARALLEL_PROCESSING_JOB_ID = 109;
	static {
		PROJECT_KEY_WITH_DISPLAY_NAME.put(Constants.OH_NO_PROJECT, Constants.NO_PROJECT);
		PROJECT_KEY_WITH_DISPLAY_NAME.put(Constants.ALL_PROJECTS, Constants.ALL_PROJECTS);
	}
	public EaiConfigBO(final Session session, final UserInformation user)
	throws FormLoaderException {
		super(session, user);
	}

	/**
	 * 
	 * @param session
	 * @param integrationId   -  Passed while editing, while create value will be -1
	 * @param integrationName - Name for this integration
	 * @param sourceSystemId  - System Id of system 1
	 * @param sourceSystemJobContext      - Context properties of agent 1
	 * @param systemId2  - System Id of system 2
	 * @param prop2      - Context properties of agent 2
	 * @param sys1Tosys2 - Whether data flow from sys1tosys2 is enabled
	 * @param sys2ToSys1 - Whether data flow from sys2tosys1 is enabled
	 * @param hmTransProcess1 -Stores the information for transformation and process definition from system1 to system2
	 * @param hmTransProcess2-Stores the information for transformation and process definition from system2 to system1
	 * @return
	 * @throws OpsHubBaseException
	 * @throws IOException
	 */
	public Integer createOrUpdateEAIConnector(final Session session,final EAIIntegrationInfo integrationInfo,final boolean isGraphQl) throws OpsHubBaseException, IOException
	{	
		// Create integration group even if there is one integration
		IntegrationGroupInfo groupInfo = IntegrationObjectformatConverter.convertUnidirectionalToIntegrationGroup(session,integrationInfo);
		IntegrationGroupBO bo = new IntegrationGroupBO(session, user);
		Integer groupId = bo.createUpdateIntegrationGroup(groupInfo, isGraphQl);
		return loadBaseFromGroup(groupId).getIntegrationBaseId();
	}

	public Integer getBaseId(final Integer integrationId) {
		EAIIntegrations integration = session.get(EAIIntegrations.class, integrationId);
		return integration.getBaseIntegration().getIntegrationBaseId();
	}

	public Integer getIntegrationGroupId(final Integer integrationId) {
		EAIIntegrations integration = session.get(EAIIntegrations.class, integrationId);
		return integration.getBaseIntegration().getIntegrationGroup().getGroupId();
	}

	/**
	 * Get only mandatory keys for job and process context
	 * @param session
	 * @param systemId : source system id
	 * @param integrationId : integraion id
	 * @param isJobContext : to differentiate between job and process context
	 * @return mandatory keys
	 * @throws OpsHubBaseException
	 */
	private List<String> getMandatoryJobOrProcessContextKeys(final Session session,
			final Integer systemId, final Integer integrationId,final boolean isJobContext,final boolean isSource) throws OpsHubBaseException{
		List<Map<String, Object>> dataList = null;
		Systems system = session.get(Systems.class, systemId);
		if (isJobContext)
			dataList = (List<Map<String, Object>>) getSystemProperties(session, systemId, integrationId, false).get(UIConstants.JOB_CONTEXT);
		else
			dataList = (List<Map<String, Object>>) getProcessContextParameters(session, system, true, integrationId,
					isSource, false, null).get(UIConstants.PROCESS_CONTEXT);
		Iterator<Map<String, Object>> jobContextListIterator = dataList.iterator();
		List<String> keyList = new ArrayList<String>();
		while (jobContextListIterator.hasNext()) {
			Map<java.lang.String, java.lang.Object> map = jobContextListIterator
					.next();
			if (Boolean.valueOf(map.get(UIConstants.MANDATORY).toString()) && isNotDependentMandatory(dataList, map))
				keyList.add(map.get(UIConstants.KEY).toString());
		}
		return keyList;

	}

	/**
	 * This method validates if exactly same workflows are associated with each event per integration, else throws error. This is done because workflow association is now independent of event types.
	 */
	private void checkProcessDefAssociation(final List<EAISourceEventInfo> eventList, final Integer sourceSystemId, final String integrationName) throws OpsHubBaseException{
		List<KeyValue> systemEvents = getSystemEvents(session, sourceSystemId);
		List<EAIProcessDefinfo> expectedProcessDefnList = eventList.get(0).getProcessDefs();
		int expectedSize = 0;
		for (EAIProcessDefinfo eaiProcessDef : expectedProcessDefnList) {
			if (eaiProcessDef.isActive()) {
				expectedSize++;
			}
		}
		HashSet<Integer> uniqueWorkflows = new HashSet<>();
		for (EAISourceEventInfo sourceEventInfo : eventList) {
			int activeProcessDefSize = 0;
			String eventId = String.valueOf(sourceEventInfo.getEventId());
			if (EaiUtility.isValidElementInListOfKeyValue(systemEvents, String.valueOf(eventId))) {
				List<EAIProcessDefinfo> processDefnList = sourceEventInfo.getProcessDefs();
				if (processDefnList == null || processDefnList.isEmpty()) {
					throw new OpsHubBaseException("012059", new Object[] { eventId, integrationName }, null);
				}
				for (EAIProcessDefinfo eaiProcessDef : processDefnList) {
					if (eaiProcessDef.isActive()) {
						activeProcessDefSize++;
						uniqueWorkflows.add(eaiProcessDef.getProcessDefnId());
					}
				}
				if (activeProcessDefSize != expectedSize) {
					// unequal active no of workflows per event
					throw new OpsHubBaseException("012061", new Object[] { integrationName }, null);
				}
			} else {
				throw new OpsHubBaseException("012058", new Object[] { eventId }, null);
			}
		}
		// different workflows associated with different events
		if (uniqueWorkflows.size() != expectedSize) {
			throw new OpsHubBaseException("012061", new Object[] { integrationName }, null);
		}

	}

	/**
	 * Delete the 'inactive' process definitions from the database irrespective of the event type
	 * @param processDefnList : List of all the process definitions associated with a particular event type
	 * @throws OpsHubBaseException
	 */
	private void deleteInActiveAssociations(final List<EAISourceEventInfo> eventList) throws OpsHubBaseException {
		for (EAISourceEventInfo sourceEventInfo : eventList) {
			List<EAIProcessDefinfo> processDefnList = sourceEventInfo.getProcessDefs();
			for (EAIProcessDefinfo processDefinfo : processDefnList) {
				Integer eventProcessDefnId = processDefinfo.getEventProcessDefnId();
				// Delete Event Process Definition ,Event Transformation Mapping& Process Context , if event process definition is marked as inactive
				if (!processDefinfo.isActive() && !eventProcessDefnId.equals(-1)) {
					deleteProcessContext(session, eventProcessDefnId);
					deleteTransMapping(session, eventProcessDefnId);
					deleteEventsLogged(session, eventProcessDefnId);
					deleteEaiEventProcessDefn(session, eventProcessDefnId);
				}
			}
		}
	}

	/**
	 * Saves the workflow and mapping associations , also the process context associated with the integration
	 */
	private void createUpdateProcessContext(final List<EAISourceEventInfo> eventList,final EAIIntegrations eaiIntegrations, final Integer sourceSystemId, final Integer targetSystemId,
			final Map<String, String> sourceJobContext, final Map<String, String> targetContext,final boolean isGraphQL,final RemoteLinkage remoteLinkage) throws OpsHubBaseException {
		Map<String, String> processCtxt = new HashMap<>(sourceJobContext);
		EaiEventProcessDefinition eventProcessDefinition = null;

		if (eventList != null && !eventList.isEmpty()) {
			checkProcessDefAssociation(eventList, sourceSystemId, eaiIntegrations.getIntegrationName());
			deleteInActiveAssociations(eventList);
			EAISourceEventInfo sourceEventsInfo = eventList.get(0);
			List<EAIProcessDefinfo> processDefnList = sourceEventsInfo.getProcessDefs();

			// Save Event Process Definition
			for (EAIProcessDefinfo processDefinfo : processDefnList) {
				Integer eventProcessDefnId = processDefinfo.getEventProcessDefnId();
				if (processDefinfo.isActive()) {
					// Save Event Process Definition only once as of now we
					// are not storing process def event type wise
					Integer processDefnId = processDefinfo.getProcessDefnId();
					eventProcessDefinition = createOrUpdateProcessDefn(session, eventProcessDefnId, processDefnId,eaiIntegrations);
					if (eventProcessDefinition != null) {
						createOrUpdateProcesContext(eaiIntegrations, sourceSystemId, targetSystemId, sourceJobContext,
								targetContext, processCtxt, eventProcessDefinition, remoteLinkage);
					}
				}
			}
			if (eventProcessDefinition != null) {
				for (int eventCnt = 0; eventCnt < eventList.size(); eventCnt++) {
					EAISourceEventInfo sourceEventInfo = eventList.get(eventCnt);
					Integer eventTypeId = sourceEventInfo.getEventId();
					List<EAIProcessDefinfo> processDefList = sourceEventInfo.getProcessDefs();
					for (EAIProcessDefinfo processDefinfo : processDefList) {
						if (processDefinfo.isActive()) {
							// Save transformation mapping
							Integer transMappingScriptId = processDefinfo.getTransformations();
							if (!isGraphQL) {
								EAIMapperIntermidiateXML interMediateXml = EAIMapperIntermidiateXML.getEaiMappingXML(session, transMappingScriptId);
								if (interMediateXml == null) {
									OpsHubLoggingUtil.error(LOGGER, "Mapping is not associated with the integration", null);
									throw new OpsInternalError("012055", new String[] {}, null);
								}
								transMappingScriptId = interMediateXml.getId();
							}
							createOrUpdateTransMapping(session, transMappingScriptId, eventTypeId, eventProcessDefinition);
						}
					}

				}
			}
		}

		else {
		throw new OpsHubBaseException("012057", new Object[] {eaiIntegrations.getIntegrationId(),eaiIntegrations.getIntegrationName(),sourceSystemId}, null);
		}
	}

	/**
	 * @param eaiIntegrations
	 * @param sourceSystemId
	 * @param targetSystemId
	 * @param sourceJobContext
	 * @param targetContext
	 * @param sourceProcessContext
	 * @param sourceProcessContextPasswordSalt
	 * @param destinationProcessContextPasswordSalt
	 * @param eventProcessDefinition
	 * @throws OpsHubBaseException
	 */
	private void createOrUpdateProcesContext(final EAIIntegrations eaiIntegrations, final Integer sourceSystemId,
			final Integer targetSystemId, final Map<String, String> sourceJobContext,
			final Map<String, String> targetContext, final Map<String, String> processCtxt,
			final EaiEventProcessDefinition eventProcessDefinition, final RemoteLinkage remoteLinkContext)
			throws OpsHubBaseException {

		String sourceProcessContextPasswordSalt = EncryptionAndDecryptionUtility.getRandomSalt();
		String destinationProcessContextPasswordSalt = EncryptionAndDecryptionUtility.getRandomSalt();

		List<String> mandatoryJobContextKeys = getMandatoryJobOrProcessContextKeys(session, sourceSystemId,eaiIntegrations.getIntegrationId(), true,true);
		List<String> mandatorySourceProcessContextKeys = getMandatoryJobOrProcessContextKeys(session, sourceSystemId,eaiIntegrations.getIntegrationId(), false,true);
		for (int jobContextCtr = 0; jobContextCtr < mandatoryJobContextKeys.size(); jobContextCtr++) {
			String key = mandatoryJobContextKeys.get(jobContextCtr);
			if (mandatorySourceProcessContextKeys.contains(key) && sourceJobContext.get(key) != null
					&& !sourceJobContext.get(key).isEmpty()) {
				processCtxt.put(key, sourceJobContext.get(key));
			}
		}
		for (Iterator<Map.Entry<String, String>> it = processCtxt.entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, String> entry = it.next();
			if (!mandatorySourceProcessContextKeys.contains(entry.getKey())
					&& !Constants.SOURCE_PROCESS_CONTEXT_KEYS.contains(entry.getKey())) {
				it.remove();
			}
		}
		if (remoteLinkContext != null) {
			processCtxt.putAll(remoteLinkContext.toSourceRemoteLinkContextMap());
			targetContext.putAll(remoteLinkContext.toTargetRemoteLinkContextMap());
		}

		Map<String, String> destProcessContext = targetContext;

		createOrUpdateProcessContext(session, eventProcessDefinition, processCtxt, sourceSystemId,
				sourceProcessContextPasswordSalt);
		createOrUpdateProcessContext(session, eventProcessDefinition, destProcessContext, targetSystemId,
				destinationProcessContextPasswordSalt);
	}

	/**
	 * This method craete/update integration context. Current implementation is
	 * smart bear specific and checks is there any change in context parameters.
	 * 
	 * @param session
	 * @param integration
	 * @param info
	 * @param sourceEntity
	 * @throws OpsHubBaseException
	 */
	private void saveIntegrationContext(final Session session, final EAIIntegrations integration,Map<String, String> ctx, final Map<String, String> destProcessContext, final EntityMeta sourceEntity, final List<ProjectMappingContext> projectMappings)
			throws OpsHubBaseException {
		if(integration.getSourceSystem().getSystemType().getSystemTypeId().equals(SystemType.SMARTBEAR) &&  !sourceEntity.getEntityInternalName().equals("testrunresults")){
			/* This is for defaulting to save Locking as Yes */
			if (ctx == null || ctx.isEmpty())
			{
				ctx = new HashMap<String, String>();
				ctx.put(LOCK, String.valueOf(UIConstants.VALUE_YES));
			}

			OIMSmartBearBO sbBo = new OIMSmartBearBO(session, integration.getSourceSystem().getSystemId());

			Set<String> mappedProjects = new HashSet<>();
			for (ProjectMappingContext prjMapping : projectMappings) {
				mappedProjects.add(prjMapping.getEp1Project());
			}
			String projectKeys = Joiner.on(',').join(mappedProjects);

			String[] projects = projectKeys.split(",");

			String entityKey = "";
			String entityInternalName = sourceEntity.getEntityInternalName();

			if ("defects".equals(entityInternalName))
				entityKey = "Bugs";
			else if ("functionalspecs".equals(entityInternalName))
				entityKey = "FunctionalSpecs";
			else if ("testcases".equals(entityInternalName))
				entityKey = "TestCases";
			else if ("tests".equals(entityInternalName))
				entityKey = "Tests";
			else if ("testsets".equals(entityInternalName))
				entityKey = "TestSets";
			else if ("releases".equals(entityInternalName))
				entityKey = "Releases";
			else if ("testconfigurations".equals(entityInternalName))
				entityKey = "TestConfigurations";
			boolean removeCall = false;

			if (ctx.containsKey(LOCK)) {
				Set<JobContext> savedCtx = integration.getSourceJobInstance().getJobContext();
				if (ctx.get(LOCK).equals(SB_LOCKED)) {
					// update if context exists.
					for (JobContext context : savedCtx) {
						if (context.getName().equals(LOCK) && !context.getValue().equals(ctx.get(LOCK))) {
							context.setValue(ctx.get(LOCK));
						}
						session.saveOrUpdate(context);
					}

					// create context
					if (savedCtx == null || savedCtx.isEmpty()) {
						JobContext context = new JobContext();
						context.setJobInstance(integration.getSourceJobInstance());
						context.setName(LOCK);
						context.setValue(ctx.get(LOCK));
						session.saveOrUpdate(context);

					}
				}
				else{
					JobContext lockedVal = null;
					if (savedCtx != null && !savedCtx.isEmpty()) {
						Iterator<JobContext> ctxItr = savedCtx.iterator();
						while (ctxItr.hasNext()) {
							JobContext ctxValue = ctxItr.next();
							if (ctxValue.getName().equals(LOCK)) {
								lockedVal = ctxValue;
								if (ctx.get(LOCK).equals("2") && ctxValue.getValue().equals("1")) {
									removeCall = true;
								}
							}
						}

					}
					if (savedCtx == null || savedCtx.isEmpty() || removeCall) {
						JobContext context = lockedVal;
						if (lockedVal == null)
							context = new JobContext();
						context.setJobInstance(integration.getSourceJobInstance());
						context.setName(LOCK);
						context.setValue("2");
						session.saveOrUpdate(context);
					}
				}
			}

			for(String project: projects)
			{

				if (removeCall) {
					if (integration.getSourceSystem().getVersion().startsWith("9.6"))
						throw new DataValidationException("013739", new String[]{integration.getSourceSystem().getVersion()}, null);
					sbBo.unsetProjectPreferences(project, entityKey);
				}

				if ("1".equals(ctx.get(LOCK))) {
					IOIMConnector adapter =null;
					try {
						/*
						 * Connector loader implementation that instantiates connectors using integration form submitted, these values are not 
						 * saved in db.
						 */

						IConnectorLoader loader = ConnectorLoaderFactory.getConnectorLoader(integration.getTargetSystem().getSystemType().getSystemTypeId(), integration.getIntegrationId(), integration.getTargetSystem().getSystemId());					
						 adapter = loader.getOIMConnector(new ProcessContextInterface() {

							@Override
								public Map<String, Object> getConnectorContext(
										final Map<String, Object> overrideProps) {
								Map<String, Object> resultMap = new HashMap<>();
								try {

										//get system properties and merger with values in integration form.
										Properties prop = EaiExtParamLoader.getExtParam(integration.getTargetSystem().getSystemId(), session);
									Iterator<Entry<Object, Object>> iterator = prop.entrySet().iterator();
									while (iterator.hasNext()) {
										Entry<Object, Object> entry = iterator.next();
										resultMap.put(entry.getKey().toString(), entry.getValue());
									}
								} catch (EAIActionHandlerException e) {
										OpsHubLoggingUtil.error(LOGGER, "Error in getting system properties for system: " + integration.getTargetSystem().getDisplayName(), e);
									throw new RuntimeException(e);
								}
								Iterator<Entry<String, String>> iterator = destProcessContext.entrySet().iterator();
									//in jira form submitted for jira work-flow comes as hashmap and connector expectes as string.
								while (iterator.hasNext()) {
									Entry<String, String> entry = iterator.next();
									if (!entry.getValue().isEmpty())
										resultMap.put(entry.getKey(), entry.getValue());
								}
								resultMap.put(Constants.SYSTEM_ID,
										integration.getTargetSystem().getSystemId().toString());
								resultMap.put(Constants.SYSTEM_VERSION, integration.getTargetSystem().getVersion());

								List<EAIProjectMappings> projectMapping = integration.getBaseIntegration()
										.getIntegrationGroup().getProjectMappingRefBase().getEaiProjectMappings();
								String projects = EaiUtility.getCommaSepatedProjectList(projectMapping, true,integration.getDirection());
								if (!projects.isEmpty())
									resultMap.put(EAIJobConstants.SmartBear.SB_USER_PROJECT, projects);

								return resultMap;
							}
						}, null);
						// create url using connector and set preferences.
							OpsHubLoggingUtil.debug(LOGGER, "Setting project preference in smartbear for locking purpose [Lock Status will be : true for URL : " + adapter.getRemoteEntityLink(Constants.ORIGINAL_ID) + " ]"  , null);
						String remoteLink = adapter.getRemoteEntityLink(Constants.ORIGINAL_ID);
						if (remoteLink != null && !remoteLink.isEmpty()) {
								sbBo.setProjectPreferences(project,remoteLink , entityKey, integration.getTargetSystem().getDisplayName());
							OpsHubLoggingUtil.debug(LOGGER, "Lock status successfully updated in smartbear", null);
						} else {
							OpsHubLoggingUtil.debug(LOGGER, "Remote entity link is empty or null", null);
						}

					} catch (ConnectorLoaderException e) {
						OpsHubLoggingUtil.error(LOGGER, "Error occurred while setting project preference for " +  entityKey, e);
						throw new OpsHubBaseException("013730", new String[] { LOCK, e.getLocalizedMessage() }, e);
					} catch (OIMAdapterException e) {
						OpsHubLoggingUtil.error(LOGGER, "Error occurred while setting project preference for " +  entityKey, e);
						throw new OpsHubBaseException("013730", new String[] { LOCK, e.getLocalizedMessage() }, e);
					}finally {
						if (adapter != null)
							adapter.cleanup();
					}
				}
			}
			ctx.remove(LOCK);
		}

		if (ctx != null && !ctx.isEmpty())
		{
			ctx.remove("query");

			for (Iterator<Entry<String, String>> iterator = ctx.entrySet().iterator(); iterator.hasNext();)
			{
				Entry<String, String> entry = iterator.next();
				// save status sync details
				saveContextForKey(session, integration, entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * This methods creates or edits the integration for given key
	 * @param session
	 * @param integration
	 * @param key
	 * @param value
	 */
	private void saveContextForKey(final Session session, final EAIIntegrations integration, final String key, final String value)
	{
		// create query to get existing context for given key
		Query contextQry = session.createQuery(FROM + JobContext.class.getName()
				+ " where jobInstance.jobInstanceId=:jobInstance " + " and name =:name");
		contextQry.setParameter(JOB_INSTANCE, integration.getSourceJobInstance().getJobInstanceId());
		contextQry.setParameter(NAME, key);

		List<JobContext> result = contextQry.list();
		if (result != null && !result.isEmpty()) // if found then edit
													// existing one
		{
			JobContext savedContext = result.get(0);
			savedContext.setValue(value);
			session.saveOrUpdate(savedContext);
		} else {
			JobContext context = new JobContext();
			context.setJobInstance(integration.getSourceJobInstance());
			context.setName(key);
			context.setValue(value);
			session.saveOrUpdate(context);
		}
	}

	/**
	 * Removes context entry from JobContext matching provided arguments
	 * 
	 * @param name
	 */
	public void removeOIMContext(final Session session, final JobInstance jobInstance, final String name) {

		Query query = session.createQuery(FROM + JobContext.class.getName()
				+ " where jobInstance.jobInstanceId =:jobInstance and name =:name");
		query.setParameter("jobInstance", jobInstance.getJobInstanceId());
		query.setString(NAME, name);
		List<JobContext> integrationContexts = query.list();

		for (JobContext oimIntegrationContext : integrationContexts) {
			session.delete(oimIntegrationContext);
		}
	}

	/**
	 * 
	 * @param name
	 * @return OIMIntegrationContext table data list where name field matches
	 *         the value specified in argument
	 */
	public List<JobContext> getSavedOIMContextByNameField(final String name) {
		Query query = session.createQuery(FROM + JobContext.class.getName() + " where name like :name");
		query.setString(NAME, name + "%");
		return query.list();
	}

	/**
	 * this return saved context for setting initial value in edit form.
	 * @param session
	 * @param integration
	 * @return
	 */
	private HashMap<String, FieldValue> getOIMContext(final Session session, final EAIIntegrations integration) {
		if(integration == null)return null;
		Set<JobContext> savedCtx = integration.getSourceJobInstance().getJobContext();
		if(savedCtx == null || savedCtx.size() == 0)return null;
		HashMap<String, FieldValue> ctx = new HashMap<String, FieldValue>();
		for (JobContext context : savedCtx) {
			ctx.put(context.getName(), new FieldValue(context.getValue()));
		}
		return ctx;
	}

	/**
	 * On given integration it will validate that is there any expired features are being used
	 * If any expired features are being used then it will pass those details.
	 * @param integration
	 * @return
	 * @throws OIMEditionException
	 */
	private String validateOIMEditionFeatures(final EAIIntegrations integration, final List<String> ignoreFeaturesFromValidation)
			throws OIMEditionException {
		String errorMessage = null;

		for (JobContext context : integration.getSourceJobInstance().getJobContext()) {
			if (context.getName().startsWith(EditionConstants.EXPIRED_FEATURES)
					&& !context.getName().startsWith(EditionConstants.EXPIRED_FEATURES_ON_DATE)
					&& !org.apache.commons.lang3.StringUtils.isEmpty(context.getValue())) {
				String expiredFeatures = context.getValue();

				List<String> features = new ArrayList<String>();
				if (ignoreFeaturesFromValidation != null && !ignoreFeaturesFromValidation.isEmpty()) {

					String[] expiredFeaturesArray = expiredFeatures.split(",");
					for (String string : expiredFeaturesArray) {
						if (!ignoreFeaturesFromValidation.contains(string)) {
							features.add(string);
						}
					}
					if (features.isEmpty()) {
						return errorMessage;
					}
					expiredFeatures = Joiner.on(",").join(features);
				}
				errorMessage = "Disabled features : "
						+ EditionDetailsLoader.getInstance().getFeaturesDesc(expiredFeatures).toString()
						+ " are configured for integration : " + integration.getIntegrationName();
			}
		}

		return errorMessage;
	}

	/**
	 * This method will change the integration status for the integration ids
	 * present in the {@code integrationIdList}. A new session will be used in
	 * internally in the method to commit the status change of each integration
	 * individually. Earlier status change was being committed in bulk, but due
	 * to 112846 this change needed to be done.
	 * 
	 * @param session
	 * @param integrationIdList
	 * @param status
	 * @return
	 * @throws OpsHubBaseException
	 * @throws SchedulerException
	 */
	private StatusValidation changeIntegrationStatus(final Session session, final List integrationIdList,
			final String status) throws OpsHubBaseException, SchedulerException {
		Integer integrationId = null;
		List<GroupMessageInfo> failedActionForJob = new ArrayList<GroupMessageInfo>();
		List<GroupMessageInfo> successActionForJob = new ArrayList<GroupMessageInfo>();
		List<GroupMessageInfo> unexpectedError = new ArrayList<GroupMessageInfo>();
		List<GroupMessageInfo> licenseError = new ArrayList<GroupMessageInfo>();
		List<String> licenseOrUnexpectedId = new ArrayList<String>();
		List<String> validationErrorMessage = new ArrayList<String>();

		String licenseOneTimeValidationErrorMessage = null;
		try {
			// License validation need only for run or execute mode , if we
			// consider for inactive mode then it will be cyclic loop as verify
			// internally calls change status
			// this license validation only need once
			if (EXECUTE.equals(status) || status.equals(RUN)) {
				LicenseVerifier licVerifier = new LicenseVerifier(integrationIdList,session);
				licVerifier.verify();
			}
		} catch (LicenseException e) {
			licenseOneTimeValidationErrorMessage = e.getMessage();
		}
		
		ExecutorService jobSchedulerPool = Executors.newFixedThreadPool(10);
		Map<GroupIntegrationInfo, Future<Void>> jobSchedlerMap = new HashMap<>();
		
		List<Integer> validIntegrationIds = validateAndGetValidIntegrationIds(session, integrationIdList, validationErrorMessage);

		// beginning new session
		Session statusChangeSession = HibernateSessionFactory.getNewOpsHubSession("changing integration status");
		// beginning new transaction to commit after every integration status change
		Transaction transaction = null;
		
		try {
			for (int listCounter = 0; listCounter < validIntegrationIds.size(); listCounter++) {
				transaction = statusChangeSession.beginTransaction();
	
				integrationId = Integer.valueOf(String.valueOf(validIntegrationIds.get(listCounter)));
				EAIIntegrations integration = statusChangeSession.get(EAIIntegrations.class, integrationId);
				String groupName = integration.getBaseIntegration().getIntegrationGroup().getGroupName();
				String integrationName = integration.getIntegrationName();
				Integer groupId = integration.getBaseIntegration().getIntegrationGroup().getGroupId();
				Integer entityPairId = integration.getBaseIntegration().getIntegrationBaseId();
				String notAvailableMessage = chackStatusAvailibility(integration, status);
				JobInstance jobInstance = integration.getSourceJobInstance();

				EAIIntegrationStatus integrationStatus = null;
				String oimEditionValidationMessage =null;
				GroupIntegrationInfo groupIntegrationInfo = new GroupIntegrationInfo(groupName, integrationName,
						groupId, integrationId, entityPairId);
				Integer jobStatus = null;
	
				// check other direction reconcile running or paused. then not
				// allowed this integration to activate
	
				try {
					String notAllowedAsRecoRunning = isReconcileConfiguredOtherDirection(statusChangeSession, integration,
							status,
							ConfigMode.INTEGRATION);
	
					if (!EaiUtility.isNullOrEmpty(notAllowedAsRecoRunning)) {
						failedActionForJob.add(new GroupMessageInfo(groupIntegrationInfo, notAllowedAsRecoRunning));
					} else if (!notAvailableMessage.isEmpty()) {
						failedActionForJob.add(new GroupMessageInfo(groupIntegrationInfo, notAvailableMessage));
					}
					else{	
						if(jobInstance.getStatus().getId().equals(JobStatus.EXPIRED) || (jobInstance.getSchedule().getEndTime()!=null && jobInstance.getSchedule().getEndTime().before(new Date()))){
							Object[] params = {jobInstance.getSchedule().getEndTime(), new Date(System.currentTimeMillis())};
							throw new ScheduleCreationException("011621", params, null);
						} else if (status.equals(RUN)) {
							// validate OIM Editions and if any expired featues are being used then don't allow to active integration
							List<String> ignoreFeaturesFromValidation = new ArrayList<String>();
							ignoreFeaturesFromValidation.add(Features.RECONCILE);
							oimEditionValidationMessage = validateOIMEditionFeatures(integration,
									ignoreFeaturesFromValidation);
							if (oimEditionValidationMessage != null) {
									licenseOrUnexpectedId.add(integration.getIntegrationName());
									licenseError.add(
										new GroupMessageInfo(groupIntegrationInfo, oimEditionValidationMessage));
									continue;
								}
	
							if (oimEditionValidationMessage == null && licenseOneTimeValidationErrorMessage == null) {
								integrationStatus = statusChangeSession.get(EAIIntegrationStatus.class,
										EAIIntegrationStatus.ACTIVE);
									//if integration's job is already running and it is attempted to run Trigger it.
									// if(jobInstance.getStatus().getId().equals(BGPJobConstants.RUNNING))
									//					schedular.doRequiredAction(session,jobInstance, BGPJobConstants.TRIGGER);
									//if it is scheduled in future then throw exception
									if (jobInstance.getSchedule().getStartTime().after(new Date()))
										throw new OpsHubBaseException("011617", null, null);
									else {
									jobStatus = BGPJobConstants.RUN;
										integration.setAnyDataInPollingCycle(true);
									}
								}
							if (oimEditionValidationMessage == null && licenseOneTimeValidationErrorMessage != null) {
								licenseError.add(
										new GroupMessageInfo(groupIntegrationInfo, licenseOneTimeValidationErrorMessage));
									licenseOrUnexpectedId.add(integrationName);
								}
								
						} else if (IN_ACTIVE.equals(status)) {
							// on in-active integration also pause its job.
							integrationStatus = statusChangeSession.get(EAIIntegrationStatus.class,
									EAIIntegrationStatus.INACTIVE);
							jobStatus = BGPJobConstants.PAUSE;
	
							JobFailureAnalyzer.removeJobFromFailureMap(String.valueOf(jobInstance.getJobInstanceId()));
							JobFailureAnalyzer.removeJobFromStuckMap(String.valueOf(jobInstance.getJobInstanceId()));
						} else if (EXECUTE.equals(status)) {
							integrationStatus = statusChangeSession.get(EAIIntegrationStatus.class,
									EAIIntegrationStatus.ACTIVE);
							jobStatus = BGPJobConstants.TRIGGER;
	
							integration.setAnyDataInPollingCycle(true);
						}
						
						// submit job action in thread
						if (jobStatus != null) {
							
							if(integration.getDeleteJobInstance() != null) {
								jobSchedulerPool.submit(new JobSchedulerThread(integration.getDeleteJobInstance().getJobInstanceId(), jobStatus));
							}
							
							jobSchedlerMap.put(groupIntegrationInfo, jobSchedulerPool
									.submit(new JobSchedulerThread(jobInstance.getJobInstanceId(), jobStatus)));
						}
	
						// Save Integration Status if no license error
						if (oimEditionValidationMessage == null && licenseOneTimeValidationErrorMessage == null) {
							if (!EXECUTE.equals(status)
									|| !EAIIntegrationStatus.ACTIVE.equals(integration.getStatus().getId())) {
								integration.setLastUpdatedOn(Calendar.getInstance().getTime());
							}
							integration.setStatus(integrationStatus);
							statusChangeSession.saveOrUpdate(integration);
							EAIIntegrationGroup group = integration.getBaseIntegration().getIntegrationGroup();
							group.setLastUpdatedOn(Calendar.getInstance().getTime());
							statusChangeSession.saveOrUpdate(group);
							successActionForJob.add(new GroupMessageInfo(groupIntegrationInfo,
									IntegrationValidation.STATUS_CHANGED_SUCCESSFULL));
							transaction.commit();
						}
					}
				} catch (OpsHubBaseException e) {
					OpsHubLoggingUtil.error(LOGGER, "Error occured while do required action on job " + 
							jobInstance.getJobName() + " for integration " + integration.getIntegrationName(), e);
					unexpectedError.add(new GroupMessageInfo(groupIntegrationInfo, e.getLocalizedMessage()));
					licenseOrUnexpectedId.add(integrationName);
				} finally {
					HibernateSessionFactory.cleanupTransaction(transaction);
				}
			}
		} finally {
			HibernateSessionFactory.cleanup(statusChangeSession, transaction);
		}

		// wait for job scheduler thread finished
		try {
			jobSchedulerPool.shutdown();
			jobSchedulerPool.awaitTermination(30, TimeUnit.MINUTES);
			Iterator<Map.Entry<GroupIntegrationInfo, Future<Void>>> jobSchedulerItr = jobSchedlerMap.entrySet()
					.iterator();
			while (jobSchedulerItr.hasNext()) {
				Map.Entry<GroupIntegrationInfo, Future<Void>> entry = jobSchedulerItr.next();
				GroupIntegrationInfo groupIntegrationInfo = new GroupIntegrationInfo(entry.getKey().getGroupName(),
						entry.getKey().getIntegrationName(), entry.getKey().getGroupId(),
						entry.getKey().getIntegrationId(), entry.getKey().getEntityPairId());
				try {
					entry.getValue().get();
				} catch (ExecutionException e) {
					unexpectedError.add(new GroupMessageInfo(groupIntegrationInfo, e.getLocalizedMessage()));
					licenseOrUnexpectedId.add(groupIntegrationInfo.getIntegrationName());
				}
			}
		} catch (InterruptedException e) {
			throw new OpsHubBaseException("011628", new String[] { status, e.getMessage() }, e);
		}

		StatusValidation statusValidation = new StatusValidation();
		statusValidation.setFailureMsgInfo(failedActionForJob);
		statusValidation.setSuccessMsgInfo(successActionForJob);
		statusValidation.setUnexpecteMsgInfo(unexpectedError);
		statusValidation.setLicenseMsgInfo(licenseError);
		statusValidation.setLicenseOrUnexpctedErrorId(licenseOrUnexpectedId);
		statusValidation.setValidationMsgInfo(validationErrorMessage);

		return statusValidation;

	}

	/*
	 * Validate the list of integration ids which are present in our instance, and return the list of ids which are present. 
	 * For those integrations, the status change will be performed 
	 */
	private List<Integer> validateAndGetValidIntegrationIds(final Session session, final List<Integer> integrationIdList,
			final List<String> validationErrorList) {
		List<Integer> validIntegrationIds = new ArrayList<>();
		//Perform only if the list is not empty. If empty, then resultset error occurs as no integrations will be fetched
		if (CollectionUtils.isNotEmpty(integrationIdList)) {
			// get group ids split list
			// this is needed when group id list is more than 100 at that time
			// oracle db can not allow >100 elements in clause.
			List<List<Integer>> idsSplitList = DbOperationsUtil.INSTANCE.splitIdsInChunks(integrationIdList);

			// iterate over group id chunk and get base ids.
			for (List<Integer> integrationIdChunk : idsSplitList) {
				List<Integer> validIds = getIntegrationIds(session, integrationIdChunk);
				if (CollectionUtils.isNotEmpty(validIds)) {
					validIntegrationIds.addAll(validIds);
				}
			}
			//Logs error for the ids which are not present in OIM and add error in validationErrorList
			logErrorIfPresent(integrationIdList, validIntegrationIds, validationErrorList);
		}
		return validIntegrationIds;
	}
	
	/**
	 * Get the integrations ids which are present in our instance 
	 * 
	 * Select integration_id from OHMT_EAI_integrations_conf where integration_id in [integrationIdList]
	 */
	private List<Integer> getIntegrationIds(final Session session, final List<Integer> integrationIdList) {
		// create query on EAIIntegrationsBase
		CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
		CriteriaQuery<Integer> query = criteriaBuilder.createQuery(Integer.class);

		Root<EAIIntegrations> fromIntegration = query.from(EAIIntegrations.class);
		query.where(criteriaBuilder.and(fromIntegration.get(EAIIntegrations_.integrationId).in(integrationIdList)));
		query.multiselect(fromIntegration.get(EAIIntegrations_.integrationId));
		return session.createQuery(query).getResultList();
	}

	private void logErrorIfPresent(final List<Integer> actualIntegrationIdList,
			final List<Integer> validIntegrationList, final List<String> validationErrorList) {
		List<Integer> tempIntegrationIdList = new ArrayList<Integer>(actualIntegrationIdList);
		tempIntegrationIdList.removeAll(validIntegrationList);
		if(CollectionUtils.isNotEmpty(tempIntegrationIdList)) {
			validationErrorList
					.add("Unidirectional Integration id(s) : " + Arrays.toString(tempIntegrationIdList.toArray())
							+ " not found");
		}
	}

	/**
	 * @param session 
	 * @param eaiIntegrations
	 * @param stateToChange
	 * @param validationFor
	 * 
	 * @return check whether other direction reconcile is completed or not if not 
	 * then return validation message for given integration.
	 * in case of validation called for reconcile then additionally check the other direction integration state as well
	 */
	private String isReconcileConfiguredOtherDirection(final Session session, final EAIIntegrations eaiIntegrations,
			final String stateToChange, final ConfigMode validationFor) {
		if (RUN.equals(stateToChange)) {
			// if state getting changed to activate
			ConfigDirection baseDirection = ConfigDirection.fetchConfigDirection(eaiIntegrations.getBaseIntegration().getDirection());
			if (ConfigDirection.BIDIRECTIONAL.equals(baseDirection)) {
				// if bidirectional then only check for other direction
				for (EAIIntegrations integrationConf : eaiIntegrations.getBaseIntegration().getIntegrations()) {
					if(!integrationConf.getIntegrationId().equals(eaiIntegrations.getIntegrationId())){
						// find other integration
						ConfigDirection directionOfIntegration = ConfigDirection
								.fetchConfigDirection(integrationConf.getDirection());
						String reconcileActiveForDirection = directionOfIntegration.getDirection();
						String messageFor = ConfigMode.RECONCILE.equals(validationFor) ? ReconcileValidation.RECONCILE
								: ConfigMode.INTEGRATION.getMode();
						if (ConfigMode.RECONCILE.equals(validationFor)
								&& EAIIntegrationStatus.ACTIVE.equals(integrationConf.getStatus().getId())) {
							// check for opposite direction integration state as
							// well if active then block the status change for
							// the incoming reconcile
							return "Cannot Activate the reconciliation, as "
									+ reconcileActiveForDirection.toLowerCase(Locale.getDefault())
									+ " direction integration is Active";
						}

						EAIReconciliations eaiReconciliations = getReconciliationInfoFromId(session,integrationConf.getIntegrationId());
						Integer reconcileState = eaiReconciliations != null
								? eaiReconciliations.getReconcileJobInstance().getStatus().getId() : null;
						if (reconcileState != null && !JobStatus.EXPIRED.equals(reconcileState)) {
							return "Cannot Activate the " + messageFor.toLowerCase(Locale.getDefault())
									+ ", as reconciliation is not completed successfully or aborted for "
									+ reconcileActiveForDirection.toLowerCase(Locale.getDefault()) + " direction";
							} 
						}
				}
				}
			}

		return null;
	}

	public StatusValidation changeStatusAPI(final Session session, final List integrationIdList, final String status)
			throws OpsHubBaseException, SchedulerException {
		return changeIntegrationStatus(session, integrationIdList, status);
	}

	public List<String> changeStatus(final Session session, final List integrationIdList, final String status,
			final boolean isGraphQL) throws OpsHubBaseException, SchedulerException {
		StatusValidation statusValidation = changeStatusAPI(session, integrationIdList, status);
		List<String> responseList = new ArrayList<String>();
		if (isGraphQL) {
			GroupMessageInfo.addGroupMessageString(responseList, statusValidation.getFailureMsgInfo(),
					IntegrationValidation.INTEGRATION, GroupMessageInfo.WARNING);
			GroupMessageInfo.addGroupMessageString(responseList, statusValidation.getUnexpecteMsgInfo(),
					IntegrationValidation.INTEGRATION, GroupMessageInfo.ERROR);
			GroupMessageInfo.addGroupMessageString(responseList, statusValidation.getLicenseMsgInfo(),
					IntegrationValidation.INTEGRATION, GroupMessageInfo.ERROR);
			GroupMessageInfo.addValidationString(responseList, statusValidation.getValidationMsgInfo());
		} else {
			String response1 = Util
					.getCommaSepartedList(GroupMessageInfo.getListOfIntegrations(statusValidation.getFailureMsgInfo()));
			if (null == response1)
				response1 = "";
			responseList.add(response1);
			if (StringUtils.isNotEmpty(response1)) {
				OpsHubLoggingUtil.debug(LOGGER, "Error while changing status for " + integrationIdList + ", status value: "
						+ status + ". Error :" + statusValidation.getFailureMsgInfo(), null);
			}
			String response2 = Util
					.getCommaSepartedList(GroupMessageInfo.getListOfIntegrations(statusValidation.getSuccessMsgInfo()));
			if (null == response2)
				response2 = "";
			responseList.add(response2);

			if (statusValidation.getLicenseOrUnexpctedErrorId().size() > 0) {
				responseList.add(Util.getCommaSepartedList(statusValidation.getLicenseOrUnexpctedErrorId()));
			}
			List<String> licenseErrorLst = GroupMessageInfo.getListOfMessages(statusValidation.getLicenseMsgInfo());
			if (licenseErrorLst.size() > 0) {
				responseList.add(Util.getCommaSepartedList(licenseErrorLst));
			}
		}
		return responseList;
	}
	
	/**
	 * This method changes the status of EAI Integration. If the status is "Run"
	 * , It changes the status of source job associated with integration to
	 * "Run" and runs the job. If the status is "InActive", It changes the
	 * status of source job associated with integration to "Pause"
	 * 
	 * @param session
	 * @param integrationId
	 * @param status
	 * @throws OpsHubBaseException
	 * @throws SchedulerException
	 */
	public List<String> changeStatus(final Session session, final List integrationIdList, final String status)
			throws OpsHubBaseException, SchedulerException {
		return changeStatus(session, integrationIdList, status, false);

	}

	/**
	 * Method for checking the available status based on the status request and
	 * integration
	 */
	private String chackStatusAvailibility(final EAIIntegrations integration,
			final String status) {
		String messageString = "";
		if (integration.getStatus().getId().equals(EAIIntegrationStatus.ACTIVE) && status.equals(RUN))
			return this.statusTransitionErrorMessage(ACTIVE);
		else if(integration.getStatus().getId().equals(EAIIntegrationStatus.INACTIVE) && (IN_ACTIVE.equals(status)||EXECUTE.equals(status)))
			if (EXECUTE.equals(status))
				return this.statusInActiveExecuteMessage();
			else
				return this.statusTransitionErrorMessage(IN_ACTIVE);
		else
			return messageString;
	}
	private String statusInActiveExecuteMessage() {
		return "Integration is " + IN_ACTIVE + ". You can't " + EXECUTE + " an Integration.";
	}
	private String statusTransitionErrorMessage(final String status) {
		return "Integration is already " + status + ". You can not change status to \"" + status
				+ "\" of an \"" + status + " Intgration";
	}
	private void validateUniqueReconcile(final Session session, final EAIReconciliations info, final boolean isEdit,
			final boolean isUniqueNameToCheck) throws DataValidationException {
		if (isUniqueNameToCheck) {
			Integer id;
			if (info.getReconciliationId() == null) {
				id = -1;
			} else {
				id = info.getReconciliationId();
			}
			if (!isUniqueReconcileName(session, info.getReconciliationName(), id, isEdit))
				throw new DataValidationException("012179", new String[] { info.getReconciliationName() }, null);
		}

	}

	/*
	 * This method checks uniqueness of EAIReconciliations name.
	 */
	private boolean isUniqueReconcileName(final Session session, final String reconciliationName, final int reconciliationId,final boolean isEdit)
			throws DataValidationException {
		boolean isUnique = false;
		Query query = null;
		if (reconciliationName != null && !reconciliationName.isEmpty()) {
			String uniqueCheckQuery = FROM + EAIReconciliations.class.getName() + " where reconciliationName =:name";
			// if edit then check for name of integration except itself.
			if (!isEdit)
				query = session.createQuery(uniqueCheckQuery);
			else
				query = session.createQuery(uniqueCheckQuery + " and reconciliationId !=" + reconciliationId);
			query.setParameter(NAME, reconciliationName);

			if (query.list().isEmpty())
				isUnique = true;
		}
		return isUnique;
	}

	/**
	 * Get current state context value from job instance
	 * 
	 * @param jobInstance
	 * @return
	 */
	private String getStoredCurrentStateContextValue(final JobInstance jobInstance) {
		String previousStoredCurrentStateContextValue = null;
		if (jobInstance != null && jobInstance.getJobContext() != null) {
			for (JobContext jContext : jobInstance.getJobContext()) {
				String key = jContext.getName();
				if (EAIJobConstants.CURRENT_STATE_SYNC_FLAG.equals(key)) {
					previousStoredCurrentStateContextValue = jContext.getValue();
					break;
				}
			}
		}
		return previousStoredCurrentStateContextValue;
	}

	/**
	 * Validation for current state flag changed or not and if changed then
	 * there should not be any failures exists on given integration
	 * 
	 * @param jobInstance
	 * @param previousStoredCurrentStateContextValue
	 * @param newCurrentStatevalue
	 * @throws DataValidationException
	 */
	private void validateCurrentStateFlag(final JobInstance jobInstance,
			final String previousStoredCurrentStateContextValue, final String newCurrentStatevalue) throws DataValidationException {
		boolean currentStateFlagEqualsAsPrevious = (StringUtils.isEmpty(previousStoredCurrentStateContextValue)  ? StringUtils.isEmpty(newCurrentStatevalue)
				: previousStoredCurrentStateContextValue.equals(newCurrentStatevalue));
		if (!currentStateFlagEqualsAsPrevious) {
			Long failedEventCount = EaiEventsLogged.getFailedEventCountForJobInstance(session,
					jobInstance.getJobInstanceId());
			if (failedEventCount > 0) {
				throw new DataValidationException("001122", new String[] { String.valueOf(failedEventCount) }, null);
			}
		}
	}

	private void createOrUpdateJobContext(final Session session, final JobInstance jobInstance, final Systems system,
			final Map<String, String> sourceJobContext) throws OpsHubBaseException {
		HasMetadata metadataImplClass = MetadataImplFactory.getMetadataImplClass(session, system);
		String allProjectKey = metadataImplClass.getAllProjectKeyName();
		String projectKey = metadataImplClass.getProjectKeyName();
		if (null != allProjectKey) {
			String isAllProjects = sourceJobContext.get(allProjectKey);
			if (isAllProjects != null && isAllProjects.equals("1") && projectKey != null)
				sourceJobContext.put(projectKey, null);
		}
		Iterator itJobContext = sourceJobContext.entrySet().iterator();

		// Get previous stored current state value
		String previousStoredCurrentStateContextValue = getStoredCurrentStateContextValue(jobInstance);

		while (itJobContext.hasNext()) {
			Map.Entry entry = (Map.Entry) itJobContext.next();
			String key = (String) entry.getKey();
			Object value = entry.getValue();

			// Validation for current state flag changed or not and if changed
			// then there should not be any failures exists on given integration
			if (EAIJobConstants.CURRENT_STATE_SYNC_FLAG.equals(key)) {
				validateCurrentStateFlag(jobInstance, previousStoredCurrentStateContextValue, (String) value);
			}
			createJobContext(session, jobInstance, key, (String) value, system);
		}
	}

	/**
	 * @param session
	 * @param jobInstance
	 * @param integrationContext
	 * @param isCreateIntegration
	 * @param pollingTimeFromEvent
	 * @throws DataValidationException
	 */
	private void insertPollingTimeKeyForAudit(final String pollingTimeFromUI,final Map<String, String> integrationContext,final boolean isCreateIntegration,final String pollingTimeFromEvent) throws DataValidationException {
		// Thu Apr 6 2017 2:29:50 EEE MMM d yyyy H:m:s
		// In case of SCM - we will not recieve any start polling time
		if (pollingTimeFromUI != null) {
			String pollingTimeFromIntegrationForm = DateUtils.convertDateToUSTimeStampString(pollingTimeFromUI,
					DateUtils.EEE_MMM_D_YYYY_H_M_S);
			if (isCreateIntegration) {
				integrationContext.put(USERT_SET_POLLING_TIME, pollingTimeFromIntegrationForm);
				integrationContext.put(INTEGRATION_POLLING_TIME, pollingTimeFromIntegrationForm);
			}
			else{			
				if (pollingTimeFromEvent != null && !pollingTimeFromEvent.equals(pollingTimeFromIntegrationForm)) {
					integrationContext.put(USERT_SET_POLLING_TIME, pollingTimeFromIntegrationForm);
					integrationContext.put(INTEGRATION_POLLING_TIME, pollingTimeFromEvent);
				}
			}
		}
	}

	/*
	 * Create a job context entry.
	 * for password fileds, first fetch all the password fileds based on system type and then
	 * based on create or edit store it in Credentials and store its reference id in the Jobcontext.
	 */
	private JobContext createJobContext(final Session session, final JobInstance jobInstance, final String name, final String value,final Systems system) throws OpsHubBaseException{

		List<JobContext> lst;
		lst = fetchJobContext(session, jobInstance.getJobInstanceId(), name);
		JobContext jc = null;
		boolean edit = false;
		if (lst.size() > 0) {
			jc = lst.get(0);
			edit = true;
		} else {
			jc = new JobContext();
		}

		CustomConfiguration subSystemConfiguration =  CustomConfigurationFactory.getSubSystemConfiguration(system.getSystemType().getSystemTypeId());
		List<String> passwordKeys = new ArrayList<String>();
		if (subSystemConfiguration != null) {
			passwordKeys = subSystemConfiguration.getPasswordKeysList(system.getSystemType().getSystemTypeId());
		}
		passwordKeys = EaiUtility.getkeys(passwordKeys);
		if (passwordKeys.contains(name)) {
			encryptPassword(session, edit, name, value, system, jc, jobInstance);
		}
		else{
			jc.setJobInstance(jobInstance);
			jc.setName(name);
			jc.setValue(value);
			session.saveOrUpdate(jc);
		}
		return jc;
	}

	/*
	 * If edit then check password is changed or not and for changed encrypt the new password and store it in credentials and its reference id is stored in jobcontext.
	 * for edit salt is not generate again so fetch the salt which was previously created.
	 * and for create generate salt and then encrypt it.
	 */

	public static void encryptPassword(final Session session,final boolean edit,final String keyPassword,final String password,final Systems system, final JobContext jc, final JobInstance jobInstance) throws OpsHubBaseException{
		try {
			String passwordRef = null;
			String salt = EncryptionAndDecryptionUtility.getRandomSalt();
			if(edit)
			{
				passwordRef = jc.getValue();
				if (system != null) {
					passwordRef = saveOrDeletePassword(session, password, passwordRef, salt);
				}
			} else {
				passwordRef = saveOrDeletePassword(session, password, null, salt);
			}
			jc.setValue(passwordRef);
			jc.setJobInstance(jobInstance);
			jc.setName(keyPassword);

			session.saveOrUpdate(jc);
		} catch (EncryptionAndDecryptionException e) {
			OpsHubLoggingUtil.error(LOGGER, "Error occured while decryption ", e);
			throw new EncryptionAndDecryptionException("011147", null, e);
		}
	}

	/**
	 * If password is non empty and password reference is null then create Credential and save.
	 * On edit, if password is made empty, and its reference exists then delete password reference and set password reference as null.
	 * On edit, if password is changed then encrypt password and save it.
	 * @param session
	 * @param password
	 * @param passwordRef
	 * @return
	 * @throws EncryptionAndDecryptionException
	 */
	private static String saveOrDeletePassword(final Session session,final String password,final String passwordRef,final String salt) throws EncryptionAndDecryptionException{
		//Create Integration and set password, Edit integration which does not have password stored and set password.
		if ((passwordRef == null || passwordRef.isEmpty()) && password != null && !password.isEmpty()) {
			String encryptedPassword = EncryptionAndDecryptionUtility.encryptionUtility(password, salt);
			Credentials credentials = new Credentials();
			credentials.setSalt(salt);
			credentials.setPassword(encryptedPassword);
			session.save(credentials);
			return credentials.getId().toString();
		}
		else{
			// edit integration which has password reference stored
			if (passwordRef != null && !passwordRef.isEmpty()) {
				Credentials savedPasswordObj = (Credentials) session.get(Credentials.class.getName(), Integer.valueOf(passwordRef));
				String savedPassword = savedPasswordObj.getPassword();

				// On edit integration, delete password stored.
				if (password == null || password.isEmpty()) {
					session.delete(savedPasswordObj);
					return null;
				}
				else{
					// On edit integration , change password
					if (!password.equals(savedPassword)) {
						savedPasswordObj.setPassword(EncryptionAndDecryptionUtility.encryptionUtility(password, savedPasswordObj.getSalt()));
						session.saveOrUpdate(savedPasswordObj);
					}
					return savedPasswordObj.getId().toString();
				}
			}
		}
		// when create integration, do not set password
		return null;
	}

	/**
	 * this method fetches the job context with key given as keyPassword and compares it with the given password
	 * if this is the encrypted password then decrypts and returns it
	 * if this the new password (no job context found or password does not match with the saved job context) given at the time of create/edit then returns itself.
	 * @param session
	 * @param keyPassword
	 * @param password
	 * @param system
	 * @param jobInstance
	 * @return
	 * @throws EncryptionAndDecryptionException
	 */
	public static String fetchDecryptedPassword(final Session session, final String keyPassword, final String password, final JobInstance jobInstance) throws EncryptionAndDecryptionException{
		String decryptedPassword = "";
		try {
			//this because now at the time of create job instance will be null. or create 
			if (jobInstance == null) {
				return password;
			}
			List<JobContext> lst = fetchJobContext(session, jobInstance.getJobInstanceId(), keyPassword);
			JobContext jc = null;
			if(lst.size() > 0)
			{
				jc = lst.get(0);
				String passwordRef = jc.getValue();
				Credentials savedPasswordObj = (Credentials) session.get(Credentials.class.getName(), Integer.valueOf(passwordRef));
				String savedPassword;
				if(savedPasswordObj == null || (savedPassword = savedPasswordObj.getPassword())==null || !savedPassword.equals(password)){
					return password;
				} else {
					decryptedPassword = EncryptionAndDecryptionUtility.decryptionUtility(savedPassword,savedPasswordObj.getSalt());
					return decryptedPassword;
				}
			} else {
				return password;
			}
		} catch (EncryptionAndDecryptionException e) {
			OpsHubLoggingUtil.error(LOGGER, "Error occured while decryption ", e);
			throw new EncryptionAndDecryptionException("011147", new String[] { "decrypting" }, e);
		}
	}

	/**
	 * This method queries job context and gets context with the given jobContextName and jobInstanceId
	 * @param session
	 * @param jobInstanceId
	 * @param jobContextName	to fetch from jobContext
	 * @return
	 */
	private static List<JobContext> fetchJobContext(final Session session, final int jobInstanceId, final String jobContextName){
		String hqlJobCtxtExists = " from " + JobContext.class.getName() + " where jobInstance.jobInstanceId=:"+PARAM_JOB_INSTANCE + " and name=:jobContextName";
		Query qryJobExists = session.createQuery(hqlJobCtxtExists);
		qryJobExists.setParameter(PARAM_JOB_INSTANCE, jobInstanceId);
		qryJobExists.setParameter("jobContextName", jobContextName);
		return qryJobExists.list();
	}
	
	private JobInstance saveDeleteJobInstance(JobInstance deleteJobInstance, final String name, final String scheduleIdString, final boolean configureDeleteJob, final UnidirectionalIntegrationData integrationData) throws OpsHubBaseException {
		
		if(configureDeleteJob) {
			if(deleteJobInstance == null) {
				deleteJobInstance = new JobInstance();
			} else {
				session.refresh(deleteJobInstance);
				if (EAIIntegrationStatus.ACTIVE.equals(deleteJobInstance.getStatus().getId())) {
					throw new OpsHubBaseException("012022", null, null);
				}
			}
			
			Job sourceJob = session.get(Job.class, Constants.DELETE_JOB_ID);
			deleteJobInstance.setJob(sourceJob);
			deleteJobInstance.setJobName(name + " DELETE");		
			JobStatus defaultJobStatus = session.get(JobStatus.class, JobStatus.PAUSED);
			deleteJobInstance.setStatus(defaultJobStatus);

			int scheduleId = Integer.parseInt(scheduleIdString);
			JobSchedule givenJobSchedule = session.get(JobSchedule.class, scheduleId);
			deleteJobInstance.setSchedule(givenJobSchedule);
			JobGroup defaultJobGroup = session.get(JobGroup.class, JobGroup.DELETE_JOB);
			deleteJobInstance.setJobGroup(defaultJobGroup);
			session.saveOrUpdate(deleteJobInstance);
		} else {
			// remove delete job when integration with updated as no
			if (integrationData.getUnidirectionalId() != null || integrationData.getUnidirectionalId() > 0) {
				EAIIntegrations integration = session.get(EAIIntegrations.class, integrationData.getUnidirectionalId());
				if (integration != null && integration.getDeleteJobInstance() != null) {
					deleteAssociatedJobInstance(integration.getDeleteJobInstance(), JobType.DELETE);
				}
			}
			return null;
		}
		return deleteJobInstance;
	}

	private JobInstance createJobInstance(final Session session,final String name, final Integer systemId,final EntityMeta sourceEntity,final Integer scheduleId){
		Systems sourceSystem = session.get(Systems.class, systemId);
		Job sourceJob = null;
		/*
			This is the temporary check, as we are migrating towards 
			the new design. Once we have completely migrated, we do not
			need the job-system-entity mappings at all.
		 */
			EAIJobMappings eaiJobMapping = getJobClassDetails(session, sourceSystem);
		sourceJob = eaiJobMapping.getJob();

		// Handling for JIRA
		if (sourceSystem.getSystemType().getSystemTypeId().equals(SystemType.JIRA)) {
			String version = sourceSystem.getVersion();
			Boolean isJIRAAboveOrEqual_5_0 = true;
			try {
				isJIRAAboveOrEqual_5_0 = JiraUtility.isVersionEqualOrAbove("5.0", version);
			} catch (OIMJiraApiException e) {
				throw new RuntimeException("Error occurred in fetching whether JIRA version id greater than or equal to 5.0" +
						"for system with version : " + version, e);
			}
			if (isJIRAAboveOrEqual_5_0 && sourceSystem.getSystemType().getSystemTypeId().equals(SystemType.JIRA) && sourceEntity.getEntityInternalName().equals(EAISystemExtensionConstants.Jira.EntityType.JIRA_SPRINT)){
				sourceJob = session.get(Job.class, 89);
				sourceJob.setJobClassName("com.opshub.eai.jira.poller.JIRASprintPoller");
			}else if (isJIRAAboveOrEqual_5_0 && sourceSystem.getSystemType().getSystemTypeId().equals(SystemType.JIRA) && sourceEntity.getEntityInternalName().equals(EAISystemExtensionConstants.Jira.EntityType.JIRA_VERSION)){
				sourceJob = session.get(Job.class, 90);
				sourceJob.setJobClassName("com.opshub.eai.jira.poller.JiraVersionPoller");
			} else if (isJIRAAboveOrEqual_5_0 && sourceEntity.getEntityInternalName()
					.equals(EAISystemExtensionConstants.Jira.EntityType.JIRA_ZEPHYR_CYCLE)) {
				sourceJob = session.get(Job.class, 106);
				sourceJob.setJobClassName("com.opshub.eai.jira.poller.JiraZephyrCyclePoller");
			} else if (isJIRAAboveOrEqual_5_0 && sourceEntity.getEntityInternalName()
					.equals(EAISystemExtensionConstants.Jira.EntityType.JIRA_ZEPHYR_EXECUTION)) {
				sourceJob = session.get(Job.class, 107);
				sourceJob.setJobClassName("com.opshub.eai.jira.poller.JiraZephyrExecutionPoller");
			} else {
				if (!isJIRAAboveOrEqual_5_0) {
					sourceJob.setJobClassName("com.opshub.eai.jira.poller.JiraIssuePoller");
				}
			}
		}else if(sourceSystem.getSystemType().getSystemTypeId().equals(SystemType.SMARTBEAR) && sourceEntity.getEntityInternalName().equals(EAISystemExtensionConstants.SmartBear.EntityType.SB_TEST_RESULT)){
			// Implementation for Smartbear Run Result Entity
			sourceJob = session.get(Job.class, 78);
		} else if (SystemType.ENTERPRISE_ARCHITECT.equals(sourceSystem.getSystemType().getSystemTypeId())
				&& (EAISystemExtensionConstants.EnterpriseArchitect.EntityType.OPERATION
						.equals(sourceEntity.getEntityInternalName())
						|| EAISystemExtensionConstants.EnterpriseArchitect.EntityType.ATTRIBUTE
								.equals(sourceEntity.getEntityInternalName()))) {
			// Implementation for EA Operation and Attribute entity
			sourceJob = session.get(Job.class, 110);
		} else if (SystemType.HPTESTDIRECTOR.equals(sourceSystem.getSystemType().getSystemTypeId())
				&& (EAISystemExtensionConstants.HPQC.EntityType.TEST_SET_FOLDER
						.equals(sourceEntity.getEntityInternalName())||EAISystemExtensionConstants.HPQC.EntityType.TEST_FOLDER.equals(sourceEntity.getEntityInternalName()))) {
			// Implementation for HPQC Test Set Folder entity
			sourceJob = session.get(Job.class, Job.SystemsJobId.HPQC_NON_HISTORY_JOB_ID);
		} else if (sourceSystem.getSystemType().getSystemTypeId().equals(SystemType.TFS)) {
			// TFS Build poller
			if (sourceEntity.getEntityInternalName().equals(EAISystemExtensionConstants.TFS.TFS_BUILD_ENTITY)) {
				sourceJob = session.get(Job.class, Job.SystemsJobId.TFS_BUILD_JOB_ID);
			}
			// TFS Git Commit poller
			else if (TFSGitConstants.GIT_COMMIT_INFO.equals(sourceEntity.getEntityInternalName())) {
				sourceJob = session.get(Job.class, Job.SystemsJobId.TFS_COMMIT_JOB_ID);
			}
			// TFS Timebox entity poller
			else if (EAISystemExtensionConstants.TFS.TIMEBOX_ENTITY.contains(sourceEntity.getEntityInternalName())) {
				sourceJob = session.get(Job.class, 92);
			} else if (TFSQueryDashboardConstants.SupportedEntities.supportedEntitiesList()
					.contains(sourceEntity.getEntityInternalName()) || TFSGitConstants.GIT_PULL_REQUEST.equals(sourceEntity.getEntityInternalName())) {
				sourceJob = session.get(Job.class, 91);
			}
		}
		else if (sourceSystem.getSystemType().getSystemTypeId().equals(SystemType.SALESFORCE) && sourceEntity.getEntityInternalName().equals(EAISystemExtensionConstants.SalesForce.EntityType.SALESFORCE_CHATTER))
		{
			sourceJob = session.get(Job.class, 88);
		}
		JobStatus defaultJobStatus = session.get(JobStatus.class, JobStatus.PAUSED);
		JobGroup defaultJobGroup = session.get(JobGroup.class, JobGroup.EAI_JOB_GROUP);

		JobInstance sourceJobInstance = new JobInstance();
		sourceJobInstance.setJob(sourceJob);
		sourceJobInstance.setJobName(name + " " + sourceSystem.getDisplayName() + " " + sourceEntity.getEntityDisplayName());		
		sourceJobInstance.setStatus(defaultJobStatus);

		JobSchedule givenJobSchedule = session.get(JobSchedule.class, scheduleId);
		sourceJobInstance.setSchedule(givenJobSchedule);

		sourceJobInstance.setJobGroup(defaultJobGroup);
		session.saveOrUpdate(sourceJobInstance);
		return sourceJobInstance;
	}

	/**
	 * @param session
	 *            hibernatesession
	 * @param sourceSystem
	 *            sourceSystem data
	 * @return poller class details
	 */
	public static EAIJobMappings getJobClassDetails(final Session session, final Systems sourceSystem) {
		String hqlGetJobMappings = FROM + EAIJobMappings.class.getName() + " where systemType.systemTypeId="+sourceSystem.getSystemType().getSystemTypeId() 
				+ AND_OH_PRODUCT_OH_PRODUCT_ID+OHProduct.OPSHUB_INTEGRATION_MANAGER_ID;
		Query queryJobMapping = session.createQuery(hqlGetJobMappings);
		return (EAIJobMappings) queryJobMapping.list().get(0);
	}

	/*
	 * Create a process context entry.(Adapter)
	 * for password fields, first fetch all the password fields based on system type and then
	 * based on create or edit store it in Credentials and store its reference id in the EAIProcessContext
	 */
	private void createOrUpdateProcessContext(final Session session, final EaiEventProcessDefinition eventProcessDefn,
			final Map<String, String> processContextmap, final Integer systemId, final String salt)
			throws OpsHubBaseException {

		EAIProcessContext processContext = null;

		Systems system = session.get(Systems.class, systemId);

		/*
		 * To delete isLinkEnable key from database if someone has not given values for it.
		 * Because if someone set it as yes and later on set it to 'select', its previous value is not cleared from db.
		 * So when someone edit the integration and try to save it will not be able as it will ask to fill the mandatory fieds dependent on this field
		 */
		String getSavedLink = " from " + EAIProcessContext.class.getName() + " where eventProcessDefinition.eventProcessDefinitionId="
				+eventProcessDefn.getEventProcessDefinitionId()+" and key='isLinkEnable' and system.systemId="+systemId;
		Query qryGetSavedLink = session.createQuery(getSavedLink);
		List<EAIProcessContext> savedLinkList = qryGetSavedLink.list();
		if (savedLinkList.size() > 0 && !processContextmap.containsKey("isLinkEnable")) {
			session.delete(savedLinkList.get(0));
		}
		List<EAIProcessContext> lst = getProcessContext(session, eventProcessDefn, systemId, null);
		for (EAIProcessContext eaiProcessContext : lst) {
			if (!processContextmap.containsKey(eaiProcessContext.getKey())) {
				processContextmap.put(eaiProcessContext.getKey(), "");
			}

		}

		jiraWorkFlowFileName = processContextmap.remove(UIConstants.JIRA_WORKFLOW_FILENAME);
		if (jiraWorkFlowFileName == null)
			jiraWorkFlowFileName = "";

		// If user has configured workflow JSON then convert JSON into workflow XML and save XML in process context
		if (processContextmap.containsKey(Constants.PIPELINE_JSON)) {
			// get JSON from process context
			String pipelineJson = processContextmap.get(Constants.PIPELINE_JSON);
			// if JSON is not null and not empty then go ahead and convert it into workflow XML
			if (pipelineJson != null && !pipelineJson.isEmpty()) {
				String workflowXML = "";
				try {
					workflowXML = PipelineJsonToWorkflow.getWorkflowXML(pipelineJson);
				} catch (PipelineException e) {
					throw new DataValidationException("017664", new String[] { e.getMessage() }, e);
				} catch (RuntimeException e) {
					throw new DataValidationException("017664", new String[] { e.getMessage() }, e);
				}
				// Add another key in process context to store overridden workflow xml
				processContextmap.put(Constants.OVERRIDE_PIPELINE, workflowXML);
			}
		}
		Iterator<Entry<String, String>> processContextItr = processContextmap.entrySet().iterator();
		while (processContextItr.hasNext()) {
			Map.Entry<String, String> entry = processContextItr.next();
			String key = entry.getKey();
			Object value = entry.getValue();

			List<EAIProcessContext> keyLst = getProcessContext(session, eventProcessDefn, systemId, key);
			boolean edit = false;
			if (keyLst.size() > 0) {
				processContext = keyLst.get(0);
				edit = true;
			} else {
				processContext = new EAIProcessContext();
			}

			CustomConfiguration subSystemConfiguration =  CustomConfigurationFactory.getSubSystemConfiguration(system.getSystemType().getSystemTypeId());
			List<String> passwordKeys = new ArrayList<String>();
			if (subSystemConfiguration != null) {
				passwordKeys = subSystemConfiguration.getPasswordKeysList(system.getSystemType().getSystemTypeId());
			}
			passwordKeys = EaiUtility.getkeys(passwordKeys);
			if (passwordKeys.contains(key)) {
				encryptPassword(session, edit, key, (String) value, system, processContext, eventProcessDefn, salt);
			}
			else {
				processContext.setSystem(system);
				processContext.setKey(key);
				if (key.equals(UIConstants.JIRA_WORKFLOW_FILE_CONT)) {
					processContext.setKey(UIConstants.JIRA_WORKFLOW);
				}
				processContext.setValue((String) value);
				processContext.setEventProcessDefinition(eventProcessDefn);
				session.saveOrUpdate(processContext);
			}
		}
	}

	public List<EAIProcessContext> getProcessContext(final Session session,
			final EaiEventProcessDefinition eventProcessDefn, final Integer systemId, final String key) {
		String savedProcessCtxtForKey = " from " + EAIProcessContext.class.getName() + " where eventProcessDefinition.eventProcessDefinitionId="
				+ eventProcessDefn.getEventProcessDefinitionId() + " and system.systemId=" + systemId;
		if (key != null) {
			savedProcessCtxtForKey = savedProcessCtxtForKey.concat(" and key=:name");
		}
		Query qryProcessExistsForKey = session.createQuery(savedProcessCtxtForKey);
		if (key != null) {
			qryProcessExistsForKey.setParameter(NAME, key);
		}
		return qryProcessExistsForKey.list();
	}

	/*
	 * If edit then check password is changed or not and for changed encrypt the new password and store it in credentials and its reference id is stored in EAIProcessContext.
	 * for edit salt is not generate again so fetch the salt which was previously created.
	 * and for create generate salt and then encrypt it.
	 */
	private void encryptPassword(final Session session, final boolean edit,
			final String key, final String password, final Systems system,
			final EAIProcessContext processContext,
			final EaiEventProcessDefinition eventProcessDefn, final String salt) throws OpsHubBaseException {
		try {
			String passwordRef = null;
			if(edit)
			{
				passwordRef = processContext.getValue();
				if (system != null) {
					passwordRef = saveOrDeletePassword(session, password, passwordRef, salt);
				}
			} else {
				passwordRef = saveOrDeletePassword(session, password, null, salt);
			}
			processContext.setValue(passwordRef);
			processContext.setSystem(system);
			processContext.setKey(key);
			processContext.setEventProcessDefinition(eventProcessDefn);
			session.saveOrUpdate(processContext);
		} catch (EncryptionAndDecryptionException e) {
			OpsHubLoggingUtil.error(LOGGER, "Error occured while decryption ", e);
			throw new EncryptionAndDecryptionException("011147", new String[] { "encrypting/decrypting" }, e);
		}
	}

	private EaiEventProcessDefinition createOrUpdateProcessDefn(final Session session, final Integer eventProcessDefnId,
			final Integer processDefnId, final EAIIntegrations integration) throws OpsHubBaseException
	{

		EaiProcessDefinition processDefinition = session.get(EaiProcessDefinition.class, processDefnId);
		EaiEventProcessDefinition eventProcessDefinition = null;

		// for cloning we need to instantiate the EaiEventProcessDefinition
		if (eventProcessDefnId.equals(-1)) {
			eventProcessDefinition = new EaiEventProcessDefinition();
		} else {
			eventProcessDefinition = session.get(EaiEventProcessDefinition.class, eventProcessDefnId);
		}

		if (eventProcessDefinition != null) {
			eventProcessDefinition.setEaiprocessDefinition(processDefinition);
			eventProcessDefinition.setIntegration(integration);
			session.saveOrUpdate(eventProcessDefinition);
		}

		return eventProcessDefinition;
	}

	private void createOrUpdateTransMapping(final Session session,final Integer transMappingScriptId,final Integer eventTypeId,final EaiEventProcessDefinition eventProcessDefinition){

		EaiEventTransformationMapping eventTransmapping = getEaiEventTransformationMapping(session,eventProcessDefinition, eventTypeId);
		if (eventTransmapping != null && transMappingScriptId == null) {
			session.delete(eventTransmapping);
			return;
		} else if (transMappingScriptId == null) {
			return;
		}
		EAIMapperIntermidiateXML transMappingScript = session.get(EAIMapperIntermidiateXML.class, transMappingScriptId);

		if (eventTransmapping == null) {
			eventTransmapping = new EaiEventTransformationMapping();
		}
		// No need to save trans-mapping if mapping script is not selected
		if (transMappingScript != null) {
			EaiEventType eventType = session.get(EaiEventType.class, eventTypeId);
			eventTransmapping.setEventType(eventType);
			eventTransmapping.setTransformationMapping(transMappingScript);
			eventTransmapping.setEventProcessDefinition(eventProcessDefinition);
			session.saveOrUpdate(eventTransmapping);
		}
	}

	private void deleteProcessContext(final Session session, final Integer eventProcessDefnId) {
		EaiEventProcessDefinition eventProcessDefinition = session.get(EaiEventProcessDefinition.class, eventProcessDefnId);
		if (eventProcessDefinition != null) {
			Iterator<EAIProcessContext> itProcessInstance = eventProcessDefinition.getContextValues().iterator();
			while (itProcessInstance.hasNext()) {
				session.delete(itProcessInstance.next());
			}
		}
	}

	private void deleteEaiEventProcessDefn(final Session session,final Integer eventProcessDefnId) throws OpsHubBaseException
	{
		EaiEventProcessDefinition eventProcessDefinition = session.get(EaiEventProcessDefinition.class, eventProcessDefnId);
		if (eventProcessDefinition != null) {
			session.delete(eventProcessDefinition);
		}
	}

	private void deleteTransMapping(final Session session, final Integer eventProcessDefnId)
	{
		String query = FROM + EaiEventType.class.getName();
		List<EaiEventType> eaiEventTypeList = session.createQuery(query).list();
		EaiEventProcessDefinition eventProcessDefinition = session.get(EaiEventProcessDefinition.class, eventProcessDefnId);
		if (eventProcessDefinition != null) {
			for (EaiEventType eaiEventType : eaiEventTypeList) {
				EaiEventTransformationMapping transformationMapping = getEaiEventTransformationMapping(session,
						eventProcessDefinition, eaiEventType.getEventTypeId());
				if (transformationMapping != null) {
					session.delete(transformationMapping);
				}
			}
		}
	}

	private void deleteEventsLogged(final Session session,final Integer eventProcessDefnId) throws OpsHubBaseException
	{
		EaiEventProcessDefinition eventProcessDefinition = session.get(EaiEventProcessDefinition.class, eventProcessDefnId);
		if (eventProcessDefinition != null) {
			Iterator<EaiEventsLogged> itr = eventProcessDefinition.getLoggedEvents().iterator();
			while (itr.hasNext()) {
				EaiEventsLogged eel = itr.next();
				deleteAssociateSrcFailed(eel.getId());
				session.delete(eel);
			}
		}
	}

	/**
	 * Loads all group level contexts like remote id, remote url, max retry
	 * count, schedule in the form
	 * 
	 * @param isFieldMapped
	 */
	public List<Map<String, Object>> getGroupContextParameters(final Session session, final Integer ep1Id,
			final Integer ep2Id, final Map<String, Object> ep1ProcessContext,
			final Map<String, Object> ep2ProcessContext, final boolean isEndPointFieldInfoNeeded) throws OpsHubBaseException {

		HashMap<String, FieldValue> initValues = new HashMap<String, FieldValue>();

		// If end point field info needed then get it
		if (isEndPointFieldInfoNeeded) {
			
			boolean isEp1Valid = isRemoteConfigurationApplicable(ep1Id);
			boolean isEp2Valid = isRemoteConfigurationApplicable(ep2Id);

			if (isEp1Valid) {
				Systems ep1 = session.load(Systems.class, ep1Id);
				EndSystemFields ep1AllFields = getEndPointsFields(true, ep1, ep1ProcessContext, true);
				Map ep1CustomField = ep1AllFields.getCustomFieldDisplay();
				initValues.put(Constants.EP1_LINK_FIELD, constructFieldValue(ep1CustomField));
				initValues.put(Constants.EP1_REMOTE_ID_FIELD, constructFieldValue(ep1CustomField));
				initValues.put(Constants.EP1_SYNC_STATUS_FIELD, constructFieldValue(ep1CustomField));
			}
			if (isEp2Valid) {
				Systems ep2 = session.load(Systems.class, ep2Id);
				EndSystemFields ep2AllFields = getEndPointsFields(true, ep2, ep2ProcessContext, false);
				Map ep2Customfield = ep2AllFields.getCustomFieldDisplay();
				initValues.put(Constants.EP2_LINK_FIELD, constructFieldValue(ep2Customfield));
				initValues.put(Constants.EP2_REMOTE_ID_FIELD, constructFieldValue(ep2Customfield));
				initValues.put(Constants.EP2_SYNC_STATUS_FIELD, constructFieldValue(ep2Customfield));
			}
		}

		Entities groupParam = session.load(Entities.class, Entities.GROUP_CONTEXT);

		FormLoader formloader = new FormLoader(session, companyId);
		List<Map<String, Object>> form = new ArrayList<Map<String, Object>>();

		String licenseEdition = EditionDetailsLoader.getInstance().getInstanceEdition(session);
		initValues.put(Constants.GLOBAL_SCHEDULE_ID,
				new FieldValue(null,
						FreeEditionGlobalScheduleLimitInitializer.getDefaultScheduleForLicense(licenseEdition), null));

		Integer viewTypeId = getViewTypeId(session, ViewTypes.FORM);
		form.addAll(formloader.getCreateOrUpdateForm(groupParam.getName(), viewTypeId, -1, false, null, initValues));
		ArrayList<Map<String, Object>> groupContextParams =(ArrayList<Map<String, Object>>) getMandatoryOrNonMandatoryFieldOnly(form, false); 

		return groupContextParams;
	}

	private boolean isRemoteConfigurationApplicable(final Integer systemId) {
		Systems system = session.load(Systems.class, systemId);
		boolean isRemoteConfigApplicable = false;
		List<SystemEntityMappingCacheInfo> list = getSystemEntityMappingList(system, Types.TYPE_PROCESS_CONTEXT);
		Iterator<SystemEntityMappingCacheInfo> lst = list.iterator();
		while (lst.hasNext()) {
			SystemEntityMappingCacheInfo sysMapping = lst.next();
			if (sysMapping.getEntityId() == 151)
				isRemoteConfigApplicable = true;
		}
		return isRemoteConfigApplicable;
	}

	private FieldValue constructFieldValue(final Map lookup) {
		FieldValue fieldValue = new FieldValue();
		fieldValue.setLookup(lookup);
		return fieldValue;
	}

	public List<SystemEntityMappingCacheInfo> getSystemEntityMappingList(final Systems system, final Integer type) {
		String key = system.getSystemType().getSystemTypeId() + "_" + type;
		synchronized (CACHED_SYSTEM_ENTITY_MAPPING) {
			if (CACHED_SYSTEM_ENTITY_MAPPING.get(key) == null) {
				String hqlGetJobParameter = FROM + SystemEntityMapping.class.getName()
						+ WHERE_SYSTEM_TYPE_SYSTEM_TYPE_ID + SYSTEM_ID + " " + AND_TYPE_TYPE_ID + type
						+ AND_OH_PRODUCT_OH_PRODUCT_ID + OHProduct.OPSHUB_INTEGRATION_MANAGER_ID;
				Query qry = session.createQuery(hqlGetJobParameter);
				qry.setInteger(SYSTEM_ID, system.getSystemType().getSystemTypeId());
				List<SystemEntityMapping> list = qry.list();
				List<SystemEntityMappingCacheInfo> cacheList = new ArrayList<>();
				for (SystemEntityMapping item : list) {
					cacheList.add(new SystemEntityMappingCacheInfo(item));
				}
				CACHED_SYSTEM_ENTITY_MAPPING.put(key, cacheList);
			}
		}

		return CACHED_SYSTEM_ENTITY_MAPPING.get(key);
	}

	/**
	 * This method returns the process context template configured for given process definition.
	 * If eventProcessDefId is -1 menas its create and we keep value to empty string else value is the value
	 * set by user previously
	 * for password fields first fetch all the password keys based on system type and then load corresponding pojo of it 
	 * and fetch password and then store it for initial field values for edit. 
	 * @throws OpsHubBaseException
	 */
	public HashMap<String, Object> getProcessContextParameters(final Session session, final Systems system,
			final boolean isBaseConfig, final Integer integrationId, final boolean isSourceSystem,
			final boolean isClone, final Map customFieldList) throws OpsHubBaseException
	{
		Integer companyId = system.getCompany().getCompanyId();
		List<SystemEntityMappingCacheInfo> list = getSystemEntityMappingList(system, Types.TYPE_PROCESS_CONTEXT);
		Iterator<SystemEntityMappingCacheInfo> lst = list.iterator();
		FormLoader formloader = new FormLoader(session, companyId);
		List<Map<String, Object>> form = new ArrayList<Map<String, Object>>();

		HashMap<String, FieldValue> initValues = new HashMap<String, FieldValue>();

		if (integrationId != -1) {

			// it will be Edit or clone case. Where the original system id which
			// is stores in database should be passed (to get process context)
			// so that process context is retrieved in case of system change as
			// well
			EAIIntegrations integrationDetails = session.get(EAIIntegrations.class, integrationId);
			Integer oldSystemId = integrationDetails.getTargetSystem().getSystemId();
			if (isSourceSystem) {
				oldSystemId = integrationDetails.getSourceSystem().getSystemId();
			}

			Set<EaiEventProcessDefinition> eventProcessDefIdList = integrationDetails.getSetProcessDefinition();
			HashMap<String, String> processContext = new HashMap<String, String>();
			if (eventProcessDefIdList != null && !eventProcessDefIdList.isEmpty()) {
				Iterator<EaiEventProcessDefinition> eveIterator = eventProcessDefIdList.iterator();
				while (eveIterator.hasNext()) {
					processContext.putAll(EaiUtility.getProcessContext(session,
							eveIterator.next(), system.getSystemId(), isClone,
							isClone, oldSystemId));
				}
			}

			Iterator<Entry<String, String>> processContextItr = processContext.entrySet().iterator();

			while (processContextItr.hasNext()) {
				Map.Entry<String, String> entry = processContextItr.next();
				String value = entry.getValue();
				FieldValue fieldValue = new FieldValue();
				fieldValue.setValue(value);
				initValues.put(entry.getKey(), fieldValue);

			}
			List<EAIProjectMappings> projectMappings = integrationDetails.getBaseIntegration().getIntegrationGroup()
					.getProjectMappingRefBase().getEaiProjectMappings();
			String projects = EaiUtility.getCommaSepatedProjectList(projectMappings, isSourceSystem,integrationDetails.getDirection());
			initValues.put(getProjectKey(system), new FieldValue(null, projects));
		}
		setSystemSpecificInitialValues(system, initValues, integrationId);
		Integer viewTypeId = getViewTypeId(session, ViewTypes.FORM);
		while (lst.hasNext()) {
			SystemEntityMappingCacheInfo systemEntityMapping = lst.next();
			FieldValue fieldValue = new FieldValue();
			FieldValue fieldValueForRemoteId = new FieldValue();
			fieldValueForRemoteId.setLookup(customFieldList);
			fieldValue.setLookup(customFieldList);
			if (initValues.containsKey(UIConstants.LINKFIELDNAME))
				fieldValue.setValue(initValues.get(UIConstants.LINKFIELDNAME).getValue());
			initValues.put(UIConstants.LINKFIELDNAME, fieldValue);

			if (initValues.containsKey(UIConstants.REMOTEIDFIELDNAME))
				fieldValueForRemoteId.setValue(initValues.get(UIConstants.REMOTEIDFIELDNAME).getValue());
			initValues.put(UIConstants.REMOTEIDFIELDNAME, fieldValueForRemoteId);
			form.addAll(formloader.getCreateOrUpdateForm(systemEntityMapping.getEntityName(), viewTypeId, integrationId,
					integrationId != -1, null, initValues));
		}
		Collections.sort(form, mapComparator);
		ArrayList<Map<String, Object>> processContextParams =(ArrayList<Map<String, Object>>) getMandatoryOrNonMandatoryFieldOnly(form, isBaseConfig); 
		ArrayList<Map<String, Object>> updatedForm = updateFormValues(processContextParams, integrationId, null,system,isSourceSystem);

		HashMap<String, Object> processContextForm = new HashMap<String, Object>();
		processContextForm.put(UIConstants.PROCESS_CONTEXT, updatedForm);
		/* Add project key for destination systems */
		if (!isSourceSystem) {
			processContextForm.put(UIConstants.PROJECT_KEY, getProjectKey(system));
		}
		return processContextForm;
	}

	public Comparator<Map<String, Object>> mapComparator = new Comparator<Map<String, Object>>() {

	@Override
		public int compare(final Map<String, Object> mapObject1, final Map<String, Object> mapObject2) {
		// TODO Auto-generated method stub
			int orderValueObject1 = mapObject1.get(UIConstants.ORDER) == null ? 0
					: (int) mapObject1.get(UIConstants.ORDER);
			int orderValueObject2 = mapObject2.get(UIConstants.ORDER) == null ? 0
					: (int) mapObject2.get(UIConstants.ORDER);

			if (orderValueObject1 < orderValueObject2) {
			return -1;
			} else if (orderValueObject1 > orderValueObject2) {
			return 1;
		} else
			return 0;
	}
	};
	/**
	 * Gets all the configuration for given integration id
	 * 
	 * @param session
	 * @param bidirectionalIntegrationId
	 * @param loadMinimal
	 * @return
	 * @throws OpsHubBaseException
	 */
	public BidirectionalIntegrationInfo getBidirectionalEditInfo(final Session session,
			final Integer bidirectionalIntegrationId, final boolean loadMinimal) throws OpsHubBaseException {
		EAIIntegrationsBase eaiIntegrations = session.get(EAIIntegrationsBase.class, bidirectionalIntegrationId);
		return getIntegrationInfoFromEAIIntegrationsBase(eaiIntegrations, loadMinimal);
	}

	public BidirectionalIntegrationInfo getIntegrationInfoFromEAIIntegrationsBase(
			final EAIIntegrationsBase eaiIntegrations,
			final boolean isLoadMinimal)
			throws OpsHubBaseException {
		EAIIntegrationInfo forward = null;
		EAIIntegrationInfo backward = null;
		ReconciliationInfo forwardReconcile = null;
		ReconciliationInfo backwardReconcile = null;

		for (EAIIntegrations eaiIntegration : eaiIntegrations.getIntegrations()) {
			int integrationId = eaiIntegration.getIntegrationId();
			EAIIntegrationInfo eaiIntegrationInfo = null;
			ReconciliationInfo recoInfo = getReconciliationInforFromEaiReconcile(
					getReconciliationInfoFromId(session, integrationId));

			if (isLoadMinimal) {
				eaiIntegrationInfo = getMinimalEditInfo(session, integrationId);
			}
			else{
				eaiIntegrationInfo = getEditInfo(session, integrationId, true);
			}

			if (ConfigDirection.FORWARD_ID.equals(eaiIntegration.getDirection())) {
				forward = eaiIntegrationInfo;
				forwardReconcile = recoInfo;
			} else {
				backward = eaiIntegrationInfo;
				backwardReconcile = recoInfo;

			}
		}
		BidirectionalIntegrationInfo bidirectionalIntegrationInfo = IntegrationObjectformatConverter
				.convertToBidirectionalFormat(session, forward, backward, forwardReconcile, backwardReconcile);
		bidirectionalIntegrationInfo.setIntegrationId(eaiIntegrations.getIntegrationBaseId());
		bidirectionalIntegrationInfo.setIntegrationName(eaiIntegrations.getIntegrationName());

		bidirectionalIntegrationInfo.setDirection(ConfigDirection.fetchConfigDirection(eaiIntegrations.getDirection()));

		return bidirectionalIntegrationInfo;
	}

	/**
	 * This method load minimal integration info which includes source context, destination context and events info
	 * currently this method is used to show list of integration on UI
	 * @param session
	 * @param integrationId
	 * @return
	 * @throws OpsHubBaseException
	 */
	public EAIIntegrationInfo getMinimalEditInfo(final Session session,final Integer integrationId)throws OpsHubBaseException{
		return getMinimalEditInfo(session, integrationId, true);
	}

	/**
	 * This method load minimal integration info which includes source context,
	 * destination context and events info currently this method is used to show
	 * list of integration on UI
	 * 
	 * @param session
	 * @param integrationId
	 * @param isGraphQL,
	 *            if True then return mapping name and mapping id otherwise
	 *            script id and name
	 * @return
	 * @throws OpsHubBaseException
	 */
	public EAIIntegrationInfo getMinimalEditInfo(final Session session, final Integer integrationId,
			final boolean isGraphQL)
			throws OpsHubBaseException {

		EAIIntegrations eaiIntegrations = session.get(EAIIntegrations.class, integrationId);
		EAIIntegrationInfo integrationInfo = getIntegrationInfoFromEAIIntegration(eaiIntegrations);
		HashMap jobCtx = EaiUtility.getJobContextValues(session,
				eaiIntegrations.getSourceJobInstance().getJobInstanceId());
		HashMap<String, Object> destProcessContext = new HashMap<>();
		destProcessContext.putAll(
				EaiUtility.getProcessContext(session, eaiIntegrations.getSetProcessDefinition().iterator().next(),
						eaiIntegrations.getTargetSystem().getSystemId(), false));
		integrationInfo.setDestProcessContext(destProcessContext);
		integrationInfo.setSourceProcessContext(new HashMap<>());
		integrationInfo.setSourceJobContext(jobCtx);
		integrationInfo.setEvents(getEventList(session, integrationId, isGraphQL, eaiIntegrations));

		return integrationInfo;
	}

	/**
	 * Gets all the configuration for given integration id
	 * 
	 * @param session
	 * @param integrationId
	 * @return
	 * @throws OpsHubBaseException
	 */
	public EAIIntegrationInfo getEditInfo(final Session session, final Integer integrationId)
			throws OpsHubBaseException {
		return getEditInfo(session, integrationId, false);
	}

	public EAIIntegrationInfo getEditInfo(final Session session, final Integer integrationId, final boolean isGraphQL)
			throws OpsHubBaseException {

		EAIIntegrations eaiIntegrations = session.get(EAIIntegrations.class, integrationId);
		// Get integration context keys
		List<String> integrationContextKeys = getIntegrationContextKeys(integrationId, eaiIntegrations);
		EAIIntegrationInfo integrationInfo = getIntegrationInfoFromEAIIntegration(eaiIntegrations);
		HashMap jobCtx = EaiUtility.getJobContextValues(session,
				eaiIntegrations.getSourceJobInstance().getJobInstanceId());
		// Remove integration context while setting up job ctx
		for (String integCtxKeys : integrationContextKeys) {
			if (jobCtx.containsKey(integCtxKeys))
				jobCtx.remove(integCtxKeys);
		}

		// set source and destination processContext

		HashMap<String, Object> destProcessContext = new HashMap<String, Object>();
		HashMap<String, Object> sourseProcessContext = new HashMap<String, Object>();

		if (eaiIntegrations.getSetProcessDefinition() != null) {
			Iterator<EaiEventProcessDefinition> processContextSet = eaiIntegrations.getSetProcessDefinition().iterator();
			if (processContextSet.hasNext()) {
				EaiEventProcessDefinition eaiEventProcessDef = processContextSet.next();
					destProcessContext.putAll(EaiUtility.getProcessContext(session, eaiEventProcessDef,
							eaiIntegrations.getTargetSystem().getSystemId(), false));

				sourseProcessContext.putAll(EaiUtility.getProcessContext(session, eaiEventProcessDef));
			}
		}
		//
		List<EAIProjectMappings> projectMappings = eaiIntegrations.getBaseIntegration().getIntegrationGroup()
				.getProjectMappingRefBase().getEaiProjectMappings();
		String sourceProjects = EaiUtility.getCommaSepatedProjectList(projectMappings, true,
				eaiIntegrations.getDirection());
		String targetProjects = EaiUtility.getCommaSepatedProjectList(projectMappings, false,
				eaiIntegrations.getDirection());
		String srcProjectKey = MetadataImplFactory.getMetadataImplClass(session, eaiIntegrations.getSourceSystem())
				.getProjectKeyName();
		String tgtProjectKey = MetadataImplFactory.getMetadataImplClass(session, eaiIntegrations.getTargetSystem())
				.getProjectKeyName();

		/*
		 * Putting List of project id in context as for retrieving meta data of
		 * source context and advance configuration it is is required.
		 */
		if (srcProjectKey != null && !Constants.ALL_PROJECTS.equals(sourceProjects))
			jobCtx.put(srcProjectKey,
					EaiUtility.getSelectedProjectList(projectMappings, true, eaiIntegrations.getDirection()));
		if (tgtProjectKey != null)
			destProcessContext.put(tgtProjectKey,
					EaiUtility.getSelectedProjectList(projectMappings, false, eaiIntegrations.getDirection()));
		IntegrationContextDataProvider dataProvider = getContextMetaDataProvider(eaiIntegrations,
				eaiIntegrations.getSourceSystem(), eaiIntegrations.getTargetSystem(), jobCtx, destProcessContext);

		/*
		 * Remove logical keys which are stored in context tables like
		 * job_context, because value for this keys will not be accepted from
		 * integration submit form Example of keys are for TFS we are storing
		 * totalCount and processedCount in job_context table but this are not
		 * actual field when we display integration form of TFS System.
		 */
		removeKeysNotInMetaData(jobCtx, dataProvider.getMetaDataForSourceContext());
		removeKeysNotInMetaData(destProcessContext, dataProvider.getMetaDataForTargetContext());

		/*
		 * Putting comma-separated projects id in context map.
		 */
		if (srcProjectKey != null && !Constants.ALL_PROJECTS.equals(sourceProjects))
			jobCtx.put(srcProjectKey, sourceProjects);
		if (tgtProjectKey != null)
			destProcessContext.put(tgtProjectKey, targetProjects);

		//Replace start polling time with last processed time
		if(jobCtx.containsKey(EAIJobConstants.POLLING_TIME) && integrationId!=-1){
			Date dt = getPollingDate(eaiIntegrations.getSourceJobInstance().getJobInstanceId());
			jobCtx.put(EAIJobConstants.POLLING_TIME,DateUtils.getUiFormatedDate(dt));
		}
		integrationInfo.setSourceJobContext(jobCtx);
		integrationInfo.setDestProcessContext(destProcessContext);
		integrationInfo.setSourceProcessContext(sourseProcessContext);

		integrationInfo.setSourceSystemProjectKey(srcProjectKey);
		integrationInfo.setDestinationSystemProjectKey(tgtProjectKey);

		List<EAISourceEventInfo> eventsList = getEventList(session, integrationId, isGraphQL, eaiIntegrations);
		integrationInfo.setEvents(eventsList);
		integrationInfo.setCriteria(getCriteria(eaiIntegrations));
		Set<JobContext> contextList = eaiIntegrations.getSourceJobInstance().getJobContext();
		HashMap<String, String> ctx = new HashMap<String, String>();
		if (contextList == null || contextList.size() == 0) {
			integrationInfo.setIntegrationContext(null);
			}
			else{
			for (JobContext context : contextList) {
				if (integrationContextKeys.contains(context.getName()))
					ctx.put(context.getName(), context.getValue());
			}
		}
		integrationInfo.setIntegrationContext(ctx);
		return integrationInfo;
	}

	private EAIIntegrationInfo getIntegrationInfoFromEAIIntegration(final EAIIntegrations eaiIntegrations) {
		EAIIntegrationInfo integrationInfo = new EAIIntegrationInfo();
		integrationInfo.setSourceJobInstanceId(eaiIntegrations.getSourceJobInstance().getJobInstanceId());
		integrationInfo.setSourceSystemId(eaiIntegrations.getSourceSystem().getSystemId());
		integrationInfo.setIntegrationId(eaiIntegrations.getIntegrationId());
		integrationInfo.setIntegrationName(eaiIntegrations.getIntegrationName());
		integrationInfo.setPollingType(eaiIntegrations.getPollingType().getPollingTypeId());
		integrationInfo.setStatus(eaiIntegrations.getStatus().getId());
		integrationInfo.setScheduleId(eaiIntegrations.getSourceJobInstance().getSchedule().getId());
		integrationInfo.setDestinationSystemId(eaiIntegrations.getTargetSystem().getSystemId());
		integrationInfo.setSourceDeleteJobInstanceId(eaiIntegrations.getDeleteJobInstance()!=null ? eaiIntegrations.getDeleteJobInstance().getJobInstanceId():null);
		ConfigMode mode = fetchIntegrationMode(eaiIntegrations);
		integrationInfo.setConfigMode(mode != null ? mode.getModeId() : ConfigMode.INTEGRATION_MODE_ID);

		return integrationInfo;
	}

	public ReconciliationInfo getReconciliationInforFromEaiReconcile(
			final EAIReconciliations eaiReconciliation) {
		ReconciliationInfo response = null;
		if (eaiReconciliation != null) {
			response = new ReconciliationInfo();
			response.setReconciliationId(eaiReconciliation.getReconciliationId());
			response.setIntegrationId(eaiReconciliation.getEaiIntegrations().getIntegrationId());
			EaiWorkflow eaiWorkflow = new EaiWorkflow();
			eaiWorkflow.setProcessDefnId(eaiReconciliation.getEaiProcessDefiniton().getProcessDefinitionId());
			eaiWorkflow.setProcessDefnName(eaiReconciliation.getEaiProcessDefiniton().getProcessDefinitionName());
			response.setWorkflowDefination(eaiWorkflow);
			response.setTargetSearchQuery(eaiReconciliation.getTargetSearchQuery());
			response.setTargetQueryCriteria(eaiReconciliation.getTargetQueryCriteria());
			response.setReconciliationName(eaiReconciliation.getReconciliationName());
			response.setObjectVersion(eaiReconciliation.getObjectVersion());
			// 1 : Paused 2: Active 3: Expired (On UI completed)
			response.setStatus(eaiReconciliation.getReconcileJobInstance().getStatus().getId());
		}
		return response;
	}

	private List<EAISourceEventInfo> getEventList(final Session session, final Integer integrationId,
			final boolean isGraphQL, final EAIIntegrations eaiIntegrations) throws OpsHubBaseException {
		List<EAISourceEventInfo> eventsList = new ArrayList<EAISourceEventInfo>();
		List<KeyValue> events = getSystemEvents(session, eaiIntegrations.getSourceSystem().getSystemId());
		for(int systemEvent=0;systemEvent<events.size();systemEvent++)
		{
			EAISourceEventInfo event = new EAISourceEventInfo();
			KeyValue eventKeyValue = events.get(systemEvent);
			Integer eventId = Integer.valueOf(eventKeyValue.getKey());
			event.setProcessDefs(getProcessDefintions(integrationId, eventId, isGraphQL));
			event.setEventId(eventId);
			event.setEventName(eventKeyValue.getValue().toString());
			Map<String, EAIEventsSkipped> skippedEvents = getExistingSkippedEvents(integrationId);
			if (skippedEvents.containsKey(eventKeyValue.getValue().toString()))
				event.setSkipped(true);
			eventsList.add(event);
		}
		return eventsList;
	}

	/**
	 * This method removes context key if its meta data is not there.
	 * 
	 * @param contextData
	 * @param list
	 */
	private void removeKeysNotInMetaData(final HashMap contextData, final List<MetaDataItem> list) {
		Iterator itr = contextData.entrySet().iterator();
		while (itr.hasNext()) {
			Entry entry = (Entry) itr.next();
			String ctxKey = (String) entry.getKey();
			boolean found = false;
			for (int metaDataIndex = 0; metaDataIndex < list.size(); metaDataIndex++) {
				if (list.get(metaDataIndex).getInternalName().equals(ctxKey)) {
					found = true;
					break;
				}
			}
			if (!found) {
				itr.remove();
			}
		}
	}

	/**
	 * @param integrationId
	 * @param eaiIntegrations
	 * @param integrationContextKeys
	 * @throws FormLoaderException
	 */
	private List<String> getIntegrationContextKeys(final Integer integrationId, final EAIIntegrations eaiIntegrations)
			throws FormLoaderException {
		List<String> integrationContextKeys = new ArrayList<>();
		List<Entities> entityNamList = getIntegrationContextEntities(eaiIntegrations.getSourceSystem());
		List<Map<String, Object>> integContextForm = getIntegrationContextForm(integrationId, entityNamList, null,
				formLoader);
		for (Map<String, Object> map : integContextForm) {
			integrationContextKeys.add((String) map.get(UIConstants.KEY));
		}
		return integrationContextKeys;
	}

	/**
	 * Get the criteria
	 * 
	 * @param integration
	 *            : integration id for which criteria has to be fetched
	 * @return CriteriaInfo pojo
	 */
	private CriteriaInfo getCriteria(final EAIIntegrations integration) {
		List<EAICriteria> list = integration.getEaiCriterias();
		CriteriaInfo criteriaInfo = null;
		if (list != null && list.size() > 0) {
			EAICriteria criteria = list.get(0);
			criteriaInfo = new CriteriaInfo(criteria.getCriteria());
		}
		return criteriaInfo;
	}

	public HashMap getCriateriaForIntegration(final Session session, final EAIIntegrations integration) {
		EAICriteria criteria = null;
		HashMap criteriaMap = new HashMap();
		List<EAICriteria> list = integration.getEaiCriterias();
		if (list != null && list.size() > 0) {
			criteria = list.get(0);
			if (criteria != null) {
				criteriaMap.put(KEY_QUERY, criteria.getCriteria());
			}
			return criteriaMap;
		}
		else 
			return null;
	}

	public List<EaiEventProcessDefinition> getAssociatedEaiEventProcessDefinitionList(final Integer integrationId) {
		String paramIntegrationId = "sourceIntegrationId";

		String hqlProcessCtxtGet = " from " + 
				EaiEventProcessDefinition.class.getName() + 
				" where integration.integrationId=:" + paramIntegrationId;

		Query qry = session.createQuery(hqlProcessCtxtGet);
		qry.setParameter(paramIntegrationId, integrationId);
		return qry.list();
	}

	/**
	 * Gets process definitions configured in given integration for particular
	 * eventId
	 * 
	 * @param integrationId
	 * @param eventId
	 * @return
	 * @throws OpsHubBaseException
	 */
	public List<EAIProcessDefinfo> getProcessDefintions(final Integer integrationId, final Integer eventId,
			final boolean isGraphQL) throws OpsHubBaseException {
		List<EaiEventProcessDefinition> lst = getAssociatedEaiEventProcessDefinitionList(integrationId);
		Iterator<EaiEventProcessDefinition> itr = lst.iterator();
		List<EAIProcessDefinfo> defs = new ArrayList<EAIProcessDefinfo>();
		while (itr.hasNext()) {
			EaiEventProcessDefinition eaiPD = itr.next();
			EAIProcessDefinfo defInfo = new EAIProcessDefinfo();

			defInfo.setEventProcessDefnId(eaiPD.getEventProcessDefinitionId());

			// Set Process Definition Info
			defInfo.setProcessDefnId(eaiPD.getEaiprocessDefinition().getProcessDefinitionId());
			defInfo.setProcessDefnName(eaiPD.getEaiprocessDefinition().getProcessDefinitionName());

			// Set Transformation Info
			Integer transformationId = getTransformations(eaiPD.getEventProcessDefinitionId(), eventId);
			String transformationName;
			if (transformationId != null) {
				EAIMapperIntermidiateXML intermediateXml = session.get(EAIMapperIntermidiateXML.class, transformationId);
				if (isGraphQL) {
					transformationName = intermediateXml.getMappingName();
				}
				else{	
					Scripts transformation = MapperUtility.getScriptFromMappingXml(session, intermediateXml,
							integrationId);
					transformationName = transformation.getScriptName();
					transformationId = transformation.getScriptId();
				}
				defInfo.setTransformationName(transformationName);
				defInfo.setTransformations(transformationId);
				defInfo.setActive(true);
				defs.add(defInfo);
			}

		}
		return defs;
	}

	/**
	 * Gets transformations configured for given event process definition id
	 * @param eventProcessDefId
	 * @return
	 */
	public Integer getTransformations(final Integer eventProcessDefId, final Integer eventId) {
		String hqlTransMapping = " from " +
				EaiEventTransformationMapping.class.getName() + 
				" where eventProcessDefinition.eventProcessDefinitionId=:eventProcessDefinitionId"
				+ " and eventType.eventTypeId=:eventId";

		Query qry = session.createQuery(hqlTransMapping);
		qry.setParameter("eventProcessDefinitionId", eventProcessDefId);
		qry.setParameter("eventId", eventId);

		List<EaiEventTransformationMapping> lst = qry.list();
		Iterator<EaiEventTransformationMapping> itr = lst.iterator();
		if(itr.hasNext())
		{
			EaiEventTransformationMapping eaiTM = itr.next();
			return eaiTM.getTransformationMapping().getId();
		}
		else
			return null;

	}

	/**
	 * Get event types based on system type
	 * @param session
	 * @param systemId
	 * @return
	 */
	public List<KeyValue> getSystemEvents(final Session session, final Integer systemId) {
		final Systems system = session.get(Systems.class, systemId);

		return system.getSystemType()
				.getSystemEvents()
				.stream()
				.sorted(Comparator.comparing(EAISystemEvents::getSystemEventId))
				.map(EAISystemEvents::getEaiEventType)
				.map(eventType -> new KeyValue(String.valueOf(eventType.getEventTypeId()), eventType.getEventType()))
				.collect(Collectors.toList());
	}

	public List<Map<String, Object>> getJobContext(final Session session, final Integer systemId,
			final EAIIntegrations integrationInfo, final boolean isClone, final HashMap<String, FieldValue> initValues)
			throws OpsHubBaseException {
		Integer jobId = -1;
		JobInstance jobInstance = null;
		Integer integrationId = -1;
		if (integrationInfo != null) {
			integrationId = integrationInfo.getIntegrationId();
			jobId = integrationInfo.getSourceJobInstance().getJobInstanceId();
			jobInstance = session.get(JobInstance.class, jobId);
		}
		Systems system = session.get(Systems.class, systemId);
		setJobContextIntialValues(jobId, initValues, system, integrationId, isClone);

		boolean isEditable = jobId != -1;

		List<Map<String, Object>> form = new ArrayList<Map<String, Object>>();

		form = getJobContextForm(system, integrationId, isEditable, initValues, jobInstance);
		if (isClone) {
			for (int formIterator = 0; formIterator < form.size(); formIterator++)
				((HashMap<String, Object>) form.get(formIterator)).put(UIConstants.DISABLED, false);
		}
		return getMandatoryOrNonMandatoryFieldOnly(form, true);
	}

	/**
	 * Get context properites for Poller Agent, based on pre-configured database
	 * for password fields first fetch all the password keys based on system type and then load corresponding pojo of it 
	 * and fetch password and then store it for initial field values for edit. 
	 * @param session
	 * @param systemId - the system for which context parameter values are queried
	 * @param jobId - While creating system jobId is -1, while editing the jobinstance
	 * stored in integrations config data will be used.
	 * @return
	 * @throws OpsHubBaseException
	 */
	public HashMap<String, Object> getSystemProperties(final Session session,final Integer systemId, final Integer integrationId,final boolean isClone) throws OpsHubBaseException{
		HashMap<String, Object> systemProperties = new HashMap<String, Object>();
		Integer jobId = -1;
		EAIIntegrations integrationInfo = null;
		if (integrationId != -1) {
			integrationInfo = session.get(EAIIntegrations.class, integrationId);
			jobId = integrationInfo.getSourceJobInstance().getJobInstanceId();
		}
		Systems system = session.get(Systems.class, systemId);
		HashMap<String, FieldValue> initValues = new HashMap<String, FieldValue>();

		boolean isEditable = jobId != -1;

		List<Map<String, Object>> mandatoryJobContext = getJobContext(session, systemId, integrationInfo, isClone,
				initValues);
		systemProperties.put(UIConstants.JOB_CONTEXT, mandatoryJobContext);

		List<Map<String, Object>> processContextParamsForSource = (List<Map<String, Object>>) getProcessContextParameters(
				session, system, true, integrationId, true, isClone, null).get(UIConstants.PROCESS_CONTEXT);
		ArrayList<Map<String, Object>> mandatoryProcessContext = (ArrayList<Map<String, Object>>) getMandatoryOrNonMandatoryFieldOnly(processContextParamsForSource,true); 
		systemProperties.put(UIConstants.PROCESS_CONTEXT, getSourceProcessContextUiMeta(mandatoryProcessContext));

		/* To add field mapping */
		Entities fieldMappingEntity = session.get(Entities.class, Entities.FIELD_MAPPING);
		Integer viewTypeId = getViewTypeId(session, AdminConstants.form);
		systemProperties.put(UIConstants.FIELD_MAPPING, formLoader.getCreateOrUpdateForm(fieldMappingEntity.getName(),
				viewTypeId, integrationId, isEditable, null, initValues));

		systemProperties.put(UIConstants.PROJECT_KEY, getProjectKey(system));
		return systemProperties;
	}

	private String getProjectKey(final Systems system) throws MetadataImplFactoryException {
		HasMetadata metadatImplClass = MetadataImplFactory.getMetadataImplClass(session, system);
		return metadatImplClass.getProjectKeyName();
	}

	private void setJobContextIntialValues(final Integer jobId, final HashMap<String, FieldValue> initValues,
			final Systems system, final Integer integrationId, final boolean isClone) throws OpsHubBaseException {
		if (jobId != -1) {
			CustomConfiguration subSystemConfiguration =  CustomConfigurationFactory.getSubSystemConfiguration(system.getSystemType().getSystemTypeId());
			List<String> passwordKeys = new ArrayList<String>();
			if (subSystemConfiguration != null)
				passwordKeys = subSystemConfiguration.getPasswordKeysList(system.getSystemType().getSystemTypeId());
			passwordKeys = EaiUtility.getkeys(passwordKeys);

			JobInstance jobInstance = session.get(JobInstance.class, jobId);

			Iterator<JobContext> itr = jobInstance.getJobContext().iterator();
			while (itr.hasNext()) {
				JobContext jc = itr.next();
				FieldValue fieldValue = new FieldValue();
				if (jc.getName().equals(EAIJobConstants.POLLING_TIME)) {
					Date dt = getPollingDate(jobId);
					if(dt!=null) fieldValue.setValue(DateUtils.getUiFormatedDate(dt));
				} else if (jc.getName().equals(EAIJobConstants.AccuWork.START_TRANS_NUM)) {
					fieldValue.setValue(getStartTransNum(jobId));
				}
				else{
					fieldValue.setValue(jc.getValue());
				}
				if (passwordKeys.contains(jc.getName())) {
					String passwordRef = jc.getValue();
					String password = "";
					if (passwordRef != null && !passwordRef.isEmpty()) {
						Credentials savedPasswordObj = (Credentials) session.get(Credentials.class.getName(), Integer.valueOf(passwordRef));
						password = savedPasswordObj.getPassword();
						// For cloning we need to show plain password in the form otherwise while saving the password it will re-encrypt it
						if (isClone) {
							password = EncryptionAndDecryptionUtility.decryptionUtility(password, savedPasswordObj.getSalt());
						}
					}
					fieldValue.setValue(password);
				}
				initValues.put(jc.getName(), fieldValue);
			}

			EAIIntegrations integrationInfo = session.get(EAIIntegrations.class, integrationId);
			Integer direction = integrationInfo.getDirection();
			List<EAIProjectMappings> projectMappings = integrationInfo.getBaseIntegration().getIntegrationGroup()
					.getProjectMappingRefBase().getEaiProjectMappings();
			String sourceProjectsIds = EaiUtility.getCommaSepatedProjectList(projectMappings, true, direction);
			if(sourceProjectsIds != null && !Constants.ALL_PROJECTS.equals(sourceProjectsIds))
			{
				String[] projectsArray = sourceProjectsIds.split(",");
				if (projectsArray != null) {
					List<String> selectedProjects = new ArrayList<String>();
					for (int eachSelProject = 0; eachSelProject < projectsArray.length; eachSelProject++)
						selectedProjects.add(projectsArray[eachSelProject]);
					initValues.put(getProjectKey(system), new FieldValue(null, selectedProjects));

				}
			}
		} else {
			Date dt = new Date();
			initValues.put(EAIJobConstants.POLLING_TIME, new FieldValue(null, DateUtils.getUiFormatedDate(dt), null));
		}
		setSystemSpecificInitialValues(system, initValues, integrationId);

	}

	/**
	 * To perform systems specific operations
	 * @param system
	 * @param initValues
	 * @throws OpsHubBaseException
	 */
	private void setSystemSpecificInitialValues(final Systems system,
			final HashMap<String, FieldValue> initValues,final Integer integrationId) throws OpsHubBaseException {
		HasMetadata metadtaImplClass = MetadataImplFactory.getMetadataImplClass(this.session, system);

		HashMap<String, Object> otherCustomMetadata = metadtaImplClass.getCustomFormMetadata();
		if (otherCustomMetadata != null && !otherCustomMetadata.isEmpty()) {
			Iterator<Map.Entry<String, Object>> otherCustomMetadataItr = otherCustomMetadata.entrySet().iterator();
			FieldValue otherMetadataFieldVal = null;
			while (otherCustomMetadataItr.hasNext()) {
				Map.Entry<java.lang.String, java.lang.Object> entry = otherCustomMetadataItr
						.next();
				String key = entry.getKey();
				HashMap value = (HashMap) entry.getValue();
				otherMetadataFieldVal = new FieldValue();

				if (!initValues.containsKey(key)) {
					otherMetadataFieldVal = new FieldValue();
					otherMetadataFieldVal.setLookup(value);
					initValues.put(entry.getKey(), otherMetadataFieldVal);

				} else {
					otherMetadataFieldVal = initValues.get(key);
					otherMetadataFieldVal.setLookup(value);
				}
			}

		}

		if (system.getSystemType().getSystemTypeId().equals(SystemType.REPLAYDIRECTOR)) {
			if (-1 == integrationId) {
				initValues.put(EAIJobConstants.ReplayDirector.MARKER_TYPE, new FieldValue(null, UIConstants.SYNC_STARRED_MARKER,null));
			}
		}
		if (system.getSystemType().getSystemTypeId().equals(SystemType.TFS)) {
			if (-1 != integrationId && !initValues.containsKey(EAIJobConstants.TFS.TFS_PROJECT_NAME)) {
				List<ProjectMeta> allProjects = metadtaImplClass.getProjectsMeta();
				List<String> projectKeyes = new ArrayList<String>();
				for (ProjectMeta projectMeta : allProjects) {
					projectKeyes.add(projectMeta.getProjectKey());

				}
				initValues.put(EAIJobConstants.TFS.TFS_PROJECT_NAME, new FieldValue(null, projectKeyes, null));

			}
		}

	}

	/**
	 * To get the source job context form
	 * @param systemId : source system id
	 * @param integrationId : integration id
	 * @param isEdit : create or edit
	 * @param initValues : initial field values
	 * @param jobInstance
	 * @return
	 * @throws OpsHubBaseException
	 */
	private List<Map<String, Object>> getJobContextForm(final Systems system, final Integer integrationId,
			final boolean isEdit, final HashMap<String, FieldValue> initValues, final JobInstance jobInstance)
			throws OpsHubBaseException {

		List<SystemEntityMappingCacheInfo> lst = getSystemEntityMappingList(system, Types.TYPE_OIM);
		if(lst.size() == 0) return Collections.emptyList();
		//if Source system is IBM ClearQuest add the DEFAULT field value "Submitter" for the widget "ClearQuest Created By User Field Name"
		if (initValues != null && system.getSystemType() != null && system.getSystemType().getSystemTypeId() != null
				&& SystemType.CLEARQUEST.equals(system.getSystemType().getSystemTypeId())
				&& (initValues.containsKey(EAIJobConstants.ClearQuest.CQ_CREATED_BY_USER)
						&& initValues.get(EAIJobConstants.ClearQuest.CQ_CREATED_BY_USER).getValue().equals(""))
				|| !isEdit)
			initValues.put(EAIJobConstants.ClearQuest.CQ_CREATED_BY_USER, new FieldValue("Submitter"));
		//If source is TFS , add default value of Regex. This will be used for regex parsing for TFS Git Commit Information
		if (initValues != null && system.getSystemType() != null && system.getSystemType().getSystemTypeId() != null
				&& SystemType.TFS.equals(system.getSystemType().getSystemTypeId()) && !isEdit)
			initValues.put(TFSGitConstants.REGEX, new FieldValue(null, TFSGitConstants.DEFAULT_REGEX));
		ArrayList<Map<String, Object>> form = getCreateUpdateForm(system, integrationId, isEdit, initValues, lst);

		return updateFormValues(form, integrationId, jobInstance, system, true);
	}

	/**
	 * @param system
	 * @param integrationId
	 * @param isEdit
	 * @param initValues
	 * @param systemEntityMappingList
	 * @return
	 * @throws FormLoaderException
	 * @throws MetadataNotFoundException
	 */
	public ArrayList<Map<String, Object>> getCreateUpdateForm(final Systems system, final Integer integrationId,
			final boolean isEdit, final HashMap<String, FieldValue> initValues,
			final List<SystemEntityMappingCacheInfo> systemEntityMappingList)
			throws FormLoaderException, MetadataNotFoundException {
		Integer companyId = system.getCompany().getCompanyId();
		FormLoader formLoader = new FormLoader(session, companyId);
		ArrayList<Map<String, Object>> form = new ArrayList<Map<String, Object>>();

		Integer viewTypeId = getViewTypeId(session, AdminConstants.form);
		for (int sysEntityMappingItr = 0; sysEntityMappingItr < systemEntityMappingList.size(); sysEntityMappingItr++)
			form.addAll(formLoader.getCreateOrUpdateForm(systemEntityMappingList.get(sysEntityMappingItr).getEntityName(), viewTypeId,
					integrationId, isEdit, null, initValues));
		return form;
	}

	public static Integer getViewTypeId(final Session session, final String viewTypeName)
			throws MetadataNotFoundException {
		if (CACHED_VIEW_TYPES.get(viewTypeName) == null) {
			String viewTypeHql = "from " + ViewTypes.class.getName() + " where viewName =:viewName";
			Query query = session.createQuery(viewTypeHql);
			query.setString("viewName", viewTypeName);
			Iterator<ViewTypes> viewItr = query.list().iterator();
			ViewTypes view = null;

			if (viewItr.hasNext()) {
				view = viewItr.next();
			}
			if (view == null) {
				OpsHubLoggingUtil.debug(LOGGER, "Invalid view type: " + viewTypeName, null);
				throw new MetadataNotFoundException("011214", new String[] { viewTypeName }, null);
			}
			CACHED_VIEW_TYPES.put(viewTypeName, view.getViewTypeId());
		}

		return CACHED_VIEW_TYPES.get(viewTypeName);
	}

	/**
	 * To get the mandatory process context fields not available in job context
	 * @param processContext
	 * @param jobContext
	 * @return
	 */
	private List<Map<String, Object>> getSourceProcessContextUiMeta(
			final ArrayList<Map<String, Object>> processContext) {

		ArrayList<Map<String, Object>> clonedProcessContext = (ArrayList<Map<String, Object>>) processContext.clone();
		Iterator<Map<String, Object>> clonedProcessContextItr = clonedProcessContext.iterator();

		while (clonedProcessContextItr.hasNext()) {
			Map<java.lang.String, java.lang.Object> map = clonedProcessContextItr
					.next();
			String key = (String) map.get(UIConstants.KEY);
			if (!Constants.SOURCE_PROCESS_CONTEXT_KEYS.contains(key)) {
				clonedProcessContextItr.remove();
			}

		}
		return clonedProcessContext;
	}

	/**
	 * To get either mandatory or non mandatory fields
	 * @param form
	 * @param isBaseConfig : to differentiate between mandatory or non mandatory
	 * @return
	 */
	private List<Map<String, Object>> getMandatoryOrNonMandatoryFieldOnly(
			final List<Map<String, Object>> form,final boolean isBaseConfig) {
		ArrayList<Map<String, Object>> mandatoryFormData = new ArrayList<Map<String, Object>>();
		ArrayList<Map<String, Object>> nonMandatoryFormData = new ArrayList<Map<String, Object>>();
		for (int formIterator = 0; formIterator < form.size(); formIterator++) {
			HashMap<String, Object> data = (HashMap<String, Object>) form.get(formIterator);
			if (data.get(UIConstants.MANDATORY) != null)
				if (Boolean.valueOf(data.get(UIConstants.MANDATORY).toString()) && isNotDependentMandatory(form, data))
					mandatoryFormData.add(data);
				else
					nonMandatoryFormData.add(data);
		}
		if (isBaseConfig)
			return mandatoryFormData;
		return nonMandatoryFormData;
	}

	/**
	 * Modify the response of formloader
	 * @param form : response of formloader
	 * @param integrationId : id of integration
	 * @param system :end system (source/target)
	 * @param isSource : true when end system involved is source
	 * @return
	 * @throws InvalidConfiguration
	 * @throws MetadataImplFactoryException
	 * @throws MetadataException
	 * @throws DataValidationException
	 * @throws EAIProcessException
	 */
	private ArrayList<Map<String, Object>> updateFormValues(final ArrayList<Map<String, Object>> form,final Integer integrationId,final JobInstance jobInstance, final Systems system,final boolean isSourceSystem) throws InvalidConfiguration, MetadataException, MetadataImplFactoryException, DataValidationException, EAIProcessException {
		ArrayList<Map<String, Object>> formClone = (ArrayList<Map<String, Object>>) form.clone();
		ArrayList<Map<String, Object>> newFormData = new ArrayList<Map<String, Object>>();
		HasMetadata metadtaImplClass = MetadataImplFactory.getMetadataImplClass(this.session, system);
		String projectKey = metadtaImplClass.getProjectKeyName();
		String projectIds = null;
		EAIIntegrations integrationInfo = null;
		if (integrationId != null && integrationId > 0 && projectKey != null) {
			integrationInfo = session.get(EAIIntegrations.class, integrationId);
			List<EAIProjectMappings> projectMappings = integrationInfo.getBaseIntegration().getIntegrationGroup()
					.getProjectMappingRefBase().getEaiProjectMappings();
			String projects = EaiUtility.getCommaSepatedProjectList(projectMappings, isSourceSystem,integrationInfo.getDirection());
			if (projects != null && !projects.isEmpty() && !Constants.ALL_PROJECTS.equals(projects))
				projectIds = projects;
		}
		for (int formIterator = 0; formIterator < form.size(); formIterator++) {
			HashMap<String, Object> data = (HashMap<String, Object>) form.get(formIterator);
			String fieldKey = (String) data.get(UIConstants.KEY);

			if (fieldKey.equals(EAIJobConstants.ISSUETYPE)) {
				data.put(UIConstants.LOOKUP, getEntityTypeLookup(projectIds, system.getSystemId(), isSourceSystem));
			}
			/*
			 * To initiate the project lookup for systems
			 */
			else if (fieldKey.equals(projectKey)) {
				projectIds = getProjectLookupAndmodifyProjectIDS(system, projectIds, true, data);
			}
			// to change the ordering of criteria
			if(data.get(UIConstants.KEY).equals(UIConstants.CONFIGURE_CRITERIA)||data.get(UIConstants.KEY).equals(UIConstants.QUERY)||data.get(UIConstants.KEY).equals(UIConstants.SYNCFIELD)){
				/*
				 * We need to make syncField editable if we save criteria as NO
				 */
				if (data.get(UIConstants.KEY).equals(UIConstants.SYNCFIELD)) {
					if (integrationInfo != null && getCriateriaForIntegration(session, integrationInfo) == null)
						data.put(UIConstants.DISABLED, false);
				}

				newFormData.add(data);
				formClone.remove(data);

			}

			if (data.get(UIConstants.KEY).equals(SourceSystem.MAX_COMMIT_ID) && integrationId != -1) {
				data.put(UIConstants.DISABLED, true);
				data.put(UIConstants.TOOLTIP, null);
			}

			if (data.get(UIConstants.KEY).equals(UIConstants.JIRA_WORKFLOW)) {
				boolean isJiraAdvanceConfigured = isJiraAdvanceConfigured(system);
				if (isSourceSystem || isJiraAdvanceConfigured) {
					formClone.remove(data);
				}
			}

			if(data.get(UIConstants.KEY).equals(UIConstants.REQPRO_CREATED_BY) && isSourceSystem)
			{
				data.put(UIConstants.MANDATORY, false);
			}
			if (data.get(UIConstants.KEY).equals(SourceSystem.PROCESSING_EVENT_INFO)) {
				// Setting readonly widget for SCM
				data.put(UIConstants.DISABLED, true);
				if (integrationId != -1 && jobInstance != null) {
					EAIIntegrations integrations = session.get(EAIIntegrations.class, integrationId);
					Integer systemTypeId = integrations.getSourceSystem().getSystemType().getSystemTypeId();
					EventTime eventTime = EaiUtility.getEventTime(session, jobInstance);
					if (eventTime == null)
						throw new InvalidConfiguration("008018", new Object[] { "Event Time" }, null);
				if(systemTypeId.equals(SystemType.CLEAR_CASE_SYSTEM) && eventTime.getLastProcessedEventTimeInTimeStamp() !=null)
						data.put(UIConstants.VALUE, eventTime.getLastProcessedEventTimeInTimeStamp());
					else if (!eventTime.getLastProcessedEventId().equals("-1"))
						data.put(UIConstants.VALUE, eventTime.getLastProcessedEventId());
				}
			}
		}
		newFormData.addAll(formClone);

		return newFormData;
	}

	/**
	 * This method sets project lookup values as well as refreshes the project
	 * selection, if applicable
	 * 
	 * @param system
	 * @param projectIds
	 * @param isCommingForProjectLookUp
	 * @param data
	 * @return
	 * @throws MetadataImplFactoryException
	 * @throws MetadataException
	 * @throws DataValidationException
	 */
	private String getProjectLookupAndmodifyProjectIDS(final Systems system, final String projectIds, final boolean isCommingForProjectLookUp,
			final HashMap<String, Object> data)
			throws MetadataImplFactoryException, MetadataException,
			DataValidationException {

		HasMetadata metadtaImplClass = MetadataImplFactory.getMetadataImplClass(this.session, system);

		Map lookupProject = new HashMap<String, ProjectMeta>();

		// retrieve projects list
		List<ProjectMeta> projectMetadata = metadtaImplClass.getProjectsMeta();
		if (projectMetadata == null)
			throw new DataValidationException("014301", new String[]{system.getDisplayName(), "Project Metadata found as null."}, null);
		Iterator<ProjectMeta> projectMetadataItr = projectMetadata.iterator();

		while (projectMetadataItr.hasNext()) {
			ProjectMeta projectMeta = projectMetadataItr.next();
			lookupProject.put(projectMeta.getProjectKey(), projectMeta);
		}

		String[] projectIdList;
		String projectIdsNew = "";
		String missingProjectIds = "";

		if (!org.apache.commons.lang3.StringUtils.isEmpty(projectIds)) {
			projectIdList = projectIds.split(",");
			for (int i = 0; i < projectIdList.length; i++) {

				//if the lookup list contains this key, then keep it in the new selection list
				if (lookupProject.containsKey(projectIdList[i])) {
					if (!projectIdsNew.isEmpty()) {
						projectIdsNew = projectIdsNew + ",";
					}
					projectIdsNew = projectIdsNew + projectIdList[i];
				}
				//else put into the list of missing project ids, so as to inform user about that
				else {
					if (!missingProjectIds.isEmpty()) {
						missingProjectIds = missingProjectIds + ",";
					}
					missingProjectIds = missingProjectIds + projectIdList[i];
				}
			}
		} else {
			//in case projectIds is null or empty, the returning string should be as it is
			projectIdsNew = projectIds;
		}

		if (isCommingForProjectLookUp) {
			if (!lookupProject.isEmpty()) {
				data.put(UIConstants.LOOKUP, lookupProject);
			}
			if (!missingProjectIds.isEmpty()) {
				data.put(EAIJobConstants.MISSING_PROJECT_IDS, missingProjectIds);
			}
		}

		return projectIdsNew;
	}

	/**
	 * To check whether jira is configured advance or not
	 * @param system
	 * @return
	 */
	private boolean isJiraAdvanceConfigured(final Systems system) {
		Iterator<SystemExtension> systemExtension = system.getSystemExtensions().iterator();
		while (systemExtension.hasNext()) {
			SystemExtension ext = systemExtension.next();
			if (ext.getName().equals("Advance Configuration") && ext.getValue().equals("1"))
				return true;
			else if (ext.getName().equals("Advanced Configuration") && ext.getValue().equals("2"))
				return false;
		}
		return false;
	}

	/**
	 * Return Entity Type map from system type.
	 * @param metadataImplClass
	 * @return
	 * @throws MetadataException
	 * @throws MetadataImplFactoryException
	 */
	public List<ListItem> getEntityTypeLookup(final String projectIdList,final Integer systemId,final boolean isJobContext) throws MetadataException, MetadataImplFactoryException{
		Systems system = session.get(Systems.class, systemId);
		HasMetadata metadataImplClass = MetadataImplFactory.getMetadataImplClass(session, system);

		List<EntityMeta> entityMetadata = metadataImplClass.getEntitiesMetadata(projectIdList, isJobContext);
		//Validating the entity type list against the available features of OIM edition and converting it to ListItem with disable flag.
		EntityTypeValidator entityTypeValidator = new EntityTypeValidator();
		return entityTypeValidator.validateAndGenerateEntityTypeList(entityMetadata);
	}

	public Map<String, Object> getDependentFieldValuesForKey(final String queryKey, final String projectKey,
			final Integer systemId) throws MetadataException,
			MetadataImplFactoryException {
		Systems system = session.get(Systems.class, systemId);
		HasMetadata metadataImplClass = MetadataImplFactory.getMetadataImplClass(session, system);
		Map<String, Object> lookup = metadataImplClass.getValuesForKey(queryKey, projectKey);

		return lookup == null ? Collections.EMPTY_MAP : lookup;
	}

	private Date getPollingDate(final Integer jobId) {
		String maxTime = getMaxLastProcessedTime(jobId);
		Date dt = null;
		if (maxTime != null) {
			dt = new Date(Long.parseLong(maxTime));
		} else {
			dt = new Date();
		}
		return dt;
	}

	/**
	 * @param jobId
	 * @return
	 */
	private String getMaxLastProcessedTime(final Integer jobId) {
		String hqlEventTime = " select max(lastProcessedEventTime) from " + EventTime.class.getName()
				+ " where integration.sourceJobInstance.jobInstanceId=:" + PARAM_JOB_INSTANCE;
		Query eventTimeQry = session.createQuery(hqlEventTime);
		eventTimeQry.setParameter(PARAM_JOB_INSTANCE, jobId);
		return eventTimeQry.uniqueResult() == null ? null : String.valueOf(eventTimeQry.uniqueResult());
	}

	public String getMaxLastProcessedTimeForIntegration(final Integer integrationId) {
		EAIIntegrations integration = session.get(EAIIntegrations.class, integrationId);
		if (integration == null) {
			return StringUtils.EMPTY;
		}
		return getMaxLastProcessedTime(integration.getSourceJobInstance().getJobInstanceId());
	}

	/**
	 * To get the last processed transaction number from event time table
	 * @param jobId : job id
	 * @return : transaction number
	 * @throws DataValidationException
	 */
	private String getStartTransNum(final Integer jobId) throws DataValidationException {
		String hqlStartTransNumExists = " select processedRevisionId from " + EventTime.class.getName()
				+ " where integration.sourceJobInstance.jobInstanceId=:" + PARAM_JOB_INSTANCE;
		Query eventTimeQry = session.createQuery(hqlStartTransNumExists);
		eventTimeQry.setParameter(PARAM_JOB_INSTANCE, jobId);
		List lstEvents = eventTimeQry.list();
		if (lstEvents.size() > 0) {
			return lstEvents.get(0).toString();
		}
		throw new DataValidationException("001117", null, null);
	}

	private EaiEventTransformationMapping getEaiEventTransformationMapping(final Session session,
			final EaiEventProcessDefinition eventProcessDefinition, final Integer eventTypeId) {
		String queryTransMapping = FROM + EaiEventTransformationMapping.class.getName() + 
				" where eventProcessDefinition.eventProcessDefinitionId =:eventProcessDefinitionId"
				+ " and eventType.eventTypeId =:eventTypeId";
		Query query = session.createQuery(queryTransMapping);
		query.setParameter("eventProcessDefinitionId", eventProcessDefinition.getEventProcessDefinitionId());
		query.setParameter("eventTypeId", eventTypeId);
		List<EaiEventTransformationMapping> listOfEventTime = query.list();
		if (listOfEventTime.size() == 1) {
			return listOfEventTime.get(0);
		} else {
			return null;
		}
	}

	/**
	 * Modifies search results and removes the link 'View Errors' from those integrations
	 * which don't have errors.
	 */
	@Override
	protected void modifySearchResults(final Session session,
			final Map<String, Object> searchResults) throws OpsHubBaseException {

		// get each result set
		List<Map<String, Object>> results = (List<Map<String, Object>>) searchResults.get(UIConstants.DATA);
		/*
		 * This BO is shared among two entities CreateEditProcessDefintion and EAISearchEntity.
		 * If search result for EAISearchEntity then process further.
		 */
		if (this.entityName.equals(AdminConstants.EAI_INEGRATION_SV)) {
			modifyIntegrationResults(searchResults, results);
		}

		else if (this.entityName.equals(AdminConstants.VIEW_RECONCILIATION)) {
			modifyReconciliationResults(searchResults, results);
		}
	}

	/**
	 * This method gets all the types of polling available there in EAIPollingType 
	 * @return hashmap containing pollingTypeId as key and polling type as value
	 */
	public Map<Integer, String> getPollingType() {
		synchronized (CACHED_POLLING_TYPE) {
			if (CACHED_POLLING_TYPE.isEmpty()) {
				// query OHMT_EAI_polling-type table to get list of polling
				// types
				String getPollingTypeQuery = " from " + EAIPollingType.class.getName();
				Query query = session.createQuery(getPollingTypeQuery);
				Iterator<EAIPollingType> pollingTypeIterator = query.list().iterator();
				// iterate through list to put each type in hash map
				while (pollingTypeIterator.hasNext()) {
					EAIPollingType pollingType = pollingTypeIterator.next();
					CACHED_POLLING_TYPE.put(pollingType.getPollingTypeId(), pollingType.getPollingType());
				}
			}
		}
		return CACHED_POLLING_TYPE;

	}

	/*
	 * This method returns status of an integration.
	 */
	public static int getStatus(final Session session, final int id) {
		EAIIntegrations integrations = session.get(EAIIntegrations.class, id);
		return integrations.getStatus().getId();
	}

	/*
	 * This method refreshes and returns status of an integration.
	 */
	public static int refreshAndGetStatus(final Session session, final int id) {
		EAIIntegrations integrations = session.get(EAIIntegrations.class, id);
		session.refresh(integrations);
		return integrations.getStatus().getId();
	}

	public static Systems getTargetSystem(final Session session, final int id) {
		EAIIntegrations integrations = session.get(EAIIntegrations.class, id);
		return integrations.getTargetSystem();
	}

	/**
	 * To get the initial criteria to set as initial field
	 * @param session : hibernate session
	 * @param systemId : id of the system
	 * @param jobId : job id
	 * @return Map<String,FieldValue>
	 */
	public Map<String, FieldValue> getCriteriaIntialVal(final Session session, final Systems systems,
			final Integer jobId) {
		if (!systems.getSystemType().getParentSystemType().getSystemTypeId().equals(SystemType.SCM)) {
			Query qry = null;
			EAICriteria criteria = null;
			// for setting criteria value in edit form.
			Map<String, FieldValue> criteriaMap = new HashMap<String, FieldValue>();
			if (jobId != -1) {
				// query to get integration id.
				qry = session.createQuery("select integrationId from " + EAIIntegrations.class.getName() +  " where sourceJobInstance.jobInstanceId=" + jobId);
				Integer integrationId = (Integer) qry.list().get(0);
				// query to get EAICriteria for this integration.
				qry = session.createQuery(FROM + EAICriteria.class.getName() + " where integration.integrationId =" + integrationId);
				// get criteria if any configured.
				if(qry.list().size()>0)criteria = (EAICriteria) qry.list().get(0);
				//				if any criteria is configured then set fields to display on edit form.  
				if (criteria != null) {
					criteriaMap.put(KEY_CONFIGURE_CRITERIA, new FieldValue(1));
					criteriaMap.put(KEY_QUERY, new FieldValue(criteria.getCriteria()));
				} else {
					//					if no criteria is configured then set configure criteria to NO.
					criteriaMap.put(KEY_CONFIGURE_CRITERIA, new FieldValue(0));
				}
			}
			return criteriaMap;
		}
		return null;
	}

	/**
	 * This method saves or updates criteria if any specified in create/edit integration form.
	 * @param session
	 * @param integrationInfo
	 * @param integration
	 * @param edit
	 */
	private void saveCriteria(final Session session, final UnidirectionalIntegrationData integrationInfo,
			final EAIIntegrations integration, final boolean edit) {
		CriteriaContext criteria = integrationInfo.getCriteria();
		EAICriteria eaiCriteria = new EAICriteria();
		if (criteria != null && criteria.isCriteriaConfigured()) {
			eaiCriteria.setIntegration(integration);
			eaiCriteria.setCriteria(criteria.getQuery());
			eaiCriteria.setSyncFieldName(CriteriaHandler.CRITERIA_FIELD);
		}
		Integer integrationId = integration.getIntegrationId();
		//		if editing then get configured criteria if any and update, if not create new criteria with received.
		if (edit) {
			List<EAICriteria> list = integration.getEaiCriterias();
			if (list != null && list.size() > 0) {
				EAICriteria oldCriteria = list.get(0);
				if (criteria != null && !criteria.isCriteriaConfigured()) {
					session.delete(oldCriteria);
				} else {
					oldCriteria.setCriteria(criteria.getQuery());
					session.saveOrUpdate(oldCriteria);
				}
			} else {
				if(criteria!=null && criteria.isCriteriaConfigured())session.saveOrUpdate(eaiCriteria);
			}
		} else {
			if (criteria != null && StringUtils.isNotEmpty(criteria.getQuery())) {
				session.saveOrUpdate(eaiCriteria);
			}
		}
	}

	private void modifyIntegrationResults(final Map<String, Object> searchResults,
			final List<Map<String, Object>> results) throws MetadataImplFactoryException {
		final Integer VIEW_FAILED_EVENTS = 2;
		final Integer ACTIVE = 4;
		final Integer INACTIVE = 3;
		final Integer EXECUTE = 6;
		final Integer DELETE = 8;
		final Integer POSTFAILURENOTIFICATION = 5;
		final Integer VIEW_REPORT = 10;
		final Integer RETRYFAILEDEVENTS = 11;

		String isNotified = FROM + EaiNotificationIntegration.class.getName() + " where integGroup.groupId =:id";
		Query isNotifiedQuery = session.createQuery(isNotified);

		for (int i = 0; i < results.size(); i++) {
			Map<String, Object> integrationResult = results.get(i);
			// get Integration Id for each row.
			int integrationId = (Integer) integrationResult.get("viewErrors");

			session.get(EAIIntegrations.class, integrationId);

			int failedEventCounts = (Integer) integrationResult.get(FAILED_EVENTS_COUNT);
			Map<Integer, Object> lookup = (Map<Integer, Object>) integrationResult.get(UIConstants.LOOKUP);
			// If it error count is zero then remove the link
			if (failedEventCounts == 0) {
				lookup.remove(VIEW_FAILED_EVENTS);
				lookup.remove(RETRYFAILEDEVENTS);
				integrationResult.put(UIConstants.LOOKUP, lookup);
			}
			// If source or target system is SCM, make 'View Report' option
			// invisible

			Query query1 = session.createQuery(SYSTEM_ID_QUERY);
			query1.setParameter(INTEGRATION_ID, integrationId);
			Object[] systemTypeIdList = (Object[]) query1.list().get(0);

			int sourceType = (Integer) systemTypeIdList[0];
			int targetType = (Integer) systemTypeIdList[1];

			if (sourceType == SystemType.SCM || targetType == SystemType.SCM) {
				lookup.remove(VIEW_REPORT);
				integrationResult.put(UIConstants.LOOKUP, lookup);
			}

			if (integrationResult.get(STATUS) != null) {
				if (integrationResult.get(STATUS).equals(UIConstants.EAIErrorManagement.ACTIVE)) {
					lookup.remove(ACTIVE);
				} else if (integrationResult.get(STATUS).equals(UIConstants.EAIErrorManagement.INACTIVE)) {
					lookup.remove(INACTIVE);
					lookup.remove(EXECUTE);
				}
			}

			isNotifiedQuery.setParameter("id", integrationId);
			if (isNotifiedQuery.list().size() > 0) {
				integrationResult.put("IsNotificationConfigured", "YES");
			} else {
				integrationResult.put("IsNotificationConfigured", "NO");
			}

			Object lastProcessedEventId = integrationResult.get(AdminConstants.LASTPROCESSED_EVENT_ID);
			String lastProcessedId = "-1";
			if (lastProcessedEventId != null) {
				lastProcessedId = lastProcessedEventId.toString();
			}

			Object lastProcessedEventTime = integrationResult.get(AdminConstants.LASTPROCESSED_EVENT_TIME);
			long finalLastProcessedTime = -1;
			if (lastProcessedEventTime != null) {
				finalLastProcessedTime = Long.parseLong(lastProcessedEventTime.toString());
			}

			Object projectKeyObj = integrationResult.get("projectKey");
			String projectKey = "";
			if (projectKeyObj != null) {
				projectKey = projectKeyObj.toString();
			}

			if ("-1".equals(lastProcessedId) || finalLastProcessedTime == -1) {//finalLastProcessedTime will be 0 only when the it is a nonTimeStampBased connector
				integrationResult.put(AdminConstants.LASTPROCESSED_EVENT_TIME, "N/A");
				integrationResult.put(AdminConstants.LASTPROCESSED_EVENT_ID, lastProcessedId);
			}
			else if (!Constants.OH_DEFAULT_PROJECT.equals(projectKey)) {
				integrationResult.put(AdminConstants.LASTPROCESSED_EVENT_TIME, new Date(finalLastProcessedTime).toString());
				integrationResult.put(AdminConstants.LASTPROCESSED_EVENT_ID, lastProcessedId + " (" + projectKey + ")");
			} else {
				integrationResult.put(AdminConstants.LASTPROCESSED_EVENT_TIME, new Date(finalLastProcessedTime).toString());
				integrationResult.put(AdminConstants.LASTPROCESSED_EVENT_ID, lastProcessedId);
			}
		}
		/*
		 * Adding the multiple operation in table
		 */
		if (searchResults.get(UIConstants.HEADER) != null) {
			List<Map<String, Object>> headers = (List<Map<String, Object>>) searchResults.get(UIConstants.HEADER);
			List<Integer> optionList = new ArrayList<Integer>();
			optionList.add(ACTIVE);
			optionList.add(INACTIVE);
			optionList.add(EXECUTE);
			optionList.add(DELETE);
			optionList.add(POSTFAILURENOTIFICATION);
			optionList.add(VIEW_FAILED_EVENTS);
			optionList.add(RETRYFAILEDEVENTS);
			setMultiOptionMenu(session, headers, optionList, false);
		}
	}


private void modifyReconciliationResults(final Map<String, Object> searchResults,final List<Map<String, Object>> results){
		final Integer ACTIVATE = 3;
		final Integer CANCEL = 4;
		final Integer VIEW_FAILURES = 5;
		final Integer EDIT = 7;
		final Integer PAUSE = 2;
		final Integer RESET_AND_ACTIVATE = 1;
		final Integer RETRY_ALL_FAILED_EVENTS = 9;
		final Integer DELETE_RECONCILIATIONS = 11;
		final Integer CONFIGURE_MAIL_NOTIFICATION = 12;

		for (int i = 0; i < results.size(); i++) {
			Map<String, Object> reconciliationResult = results.get(i);
			// get Reconciliation Id for each row.
			int reconciliationId = (Integer) reconciliationResult.get("viewErrors");
			EAIReconciliations eaiReconciliation = (EAIReconciliations)session.load(EAIReconciliations.class.getName(), reconciliationId);
			Query eventTimeRecords = session.createQuery(
					" from " + EventTime.class.getName() + " where integration.sourceJobInstance.jobInstanceId=:"
							+ PARAM_JOB_INSTANCE);
			eventTimeRecords.setParameter(PARAM_JOB_INSTANCE,
					eaiReconciliation.getReconcileJobInstance().getJobInstanceId());
			Query eventsLoggedRecords = session.createQuery(" select count(*) FROM "+ EaiEventsLogged.class.getName() +
					" WHERE sourceJobInstance.jobInstanceId=:jobInstanceId");
			eventsLoggedRecords.setParameter(PARAM_JOB_INSTANCE, eaiReconciliation.getReconcileJobInstance().getJobInstanceId());
			Long errorCount = (Long) eventsLoggedRecords.list().get(0);

			reconciliationResult.put(AdminConstants.FAILED_EVENT_COUNT, eventsLoggedRecords.list().get(0));

			int maxProcssedId = 0;
			String projectKey = "";
			for (EventTime et : (List<EventTime>) eventTimeRecords.list()) {
				int lstProcessedId = Integer.parseInt(et.getLastProcessedEventId());
				if (lstProcessedId > maxProcssedId) {
					maxProcssedId = lstProcessedId;
					projectKey = et.getProjectKey();
				}
				if (maxProcssedId > 0) {
					OIMEntityInfo eInfo = session.get(OIMEntityInfo.class, maxProcssedId);
					String appendProjectKey = "";
					if (!Constants.OH_DEFAULT_PROJECT.equals(projectKey)) {
						appendProjectKey = projectKey + "/";
					}
					reconciliationResult.put(AdminConstants.LASTPROCESSED_EVENT_ID, appendProjectKey + eInfo.getEntityInternalId());
				}

			}

			Map<Integer, Object> lookup = (Map<Integer, Object>) reconciliationResult.get(UIConstants.LOOKUP);
			if (errorCount == 0) {
				lookup.remove(VIEW_FAILURES);
				lookup.remove(RETRY_ALL_FAILED_EVENTS);
			}
			if (reconciliationResult.get(STATUS) != null) {
				Map status = (Map) reconciliationResult.get(STATUS);

				if (status.size() > 0) {

					String statusVal = (String) status.values().iterator().next();

					if ("ACTIVE".equals(statusVal)) {
						lookup.remove(ACTIVATE);
						lookup.remove(EDIT);
						lookup.remove(RESET_AND_ACTIVATE);
					} else if ("PAUSED".equals(statusVal)) {
						lookup.remove(RESET_AND_ACTIVATE);
						lookup.remove(PAUSE);
					} else if ("EXPIRED".equals(statusVal)) {
						lookup.remove(CANCEL);
						lookup.remove(PAUSE);
						lookup.remove(ACTIVATE);
						lookup.remove(EDIT);
					}
				}
			}
		}
		/*
		 * Adding the multiple operation in table
		 */
		if (searchResults.get(UIConstants.HEADER) != null) {
			List<Map<String, Object>> headers = (List<Map<String, Object>>) searchResults.get(UIConstants.HEADER);
			List<Integer> optionList = new ArrayList<Integer>();
			optionList.add(ACTIVATE);
			optionList.add(CANCEL);
			optionList.add(PAUSE);
			optionList.add(RETRY_ALL_FAILED_EVENTS);
			optionList.add(CONFIGURE_MAIL_NOTIFICATION);
			optionList.add(VIEW_FAILURES);
			optionList.add(DELETE_RECONCILIATIONS);

			setMultiOptionMenu(session, headers, optionList, false);
		}

	}

	@Override
	protected void checkDeleteAuth(final Session session, final String entityName,
			final Integer userId, final Integer id) throws OpsHubBaseException {
		EAIIntegrations eaiIntegration = session.get(EAIIntegrations.class, id);
		if (eaiIntegration.getStatus().getId().equals(EAIIntegrationStatus.ACTIVE))
			throw new DataValidationException("001113", null, null);
	}

	@Override
	protected void delete(final Session session, final String entityName, final Integer id)
	throws OpsHubBaseException {		
		deleteIntegration(id, true);
	}

	/**
	 * Method for delete the list on integration if they are not in active state
	 * returns the string for activated integration Id
	 */
	@Override
	public List<String> delete(final Session session, final UserInformation user, final String entityName, final List ids) throws OpsHubBaseException {
		List<String> responseList = new ArrayList<String>();
		List<String> successfullyDeleted = new ArrayList<String>();
		List<String> canNotDeleted = new ArrayList<String>();
		for (int integrationCounter = 0; integrationCounter < ids.size(); integrationCounter++) {
			Integer integrationId = Integer.valueOf(ids.get(integrationCounter).toString());
			int status = getStatus(session, integrationId);
			if (status == EAIIntegrationStatus.ACTIVE)
				canNotDeleted.add(integrationId.toString());
			else {
				deleteIntegration(integrationId, true);
				successfullyDeleted.add(integrationId.toString());
			}
		}
		responseList.add(Util.getCommaSepartedList(canNotDeleted));
		return responseList;
	}

	/**
	 * This method is used by GWT. It deletes base integration, project mapping reference base, project mapping tuples associated with project mapping refr base, and finally the integration group
	 * @param baseId
	 * @throws FormLoaderException
	 */
	private void deleteIntegrationBaseAndGroup(final Integer baseId, final boolean deleteGroup) throws OpsHubBaseException{
		EAIIntegrationsBase base = session.get(EAIIntegrationsBase.class, baseId);
		EAIIntegrationGroup integrationGroup = base.getIntegrationGroup();
		// delete Base Integration
		deleteBaseIntegration(base.getIntegrationBaseId());
		// Fetch all the base integrations in the integration group
		Query query = session.createQuery("from " + EAIIntegrationsBase.class.getName() +" where integrationGroup.groupId=" + integrationGroup.getGroupId());
		//If other bidirectional integrations are associated with the group, group can not be deleted
		if (query.list().size() >= 1 || !deleteGroup)
			return;
		IntegrationGroupBO groupBo = new IntegrationGroupBO(session, user);
		// delete integration group context
		groupBo.deleteIntegrationGroupContext(integrationGroup);
		// delete notification related to the integration
		deletePostFailureNotification(integrationGroup.getGroupId());
		// finally delete the integration group
		session.delete(integrationGroup);
		// delete project mapping tuples
		groupBo.deleteProjectMappingTuples(integrationGroup.getProjectMappingRefBase().getEaiProjectMappings());
		// delete project mapping base associated with the integration group
		session.delete(integrationGroup.getProjectMappingRefBase());
	}

	/**
	 * Deletes the base integration ; if unidirectional integrations associated with it are deleted.
	 */
	private void deleteBaseIntegration(final Integer baseId) {
		Query integrationsAssociatedQuery = session.createQuery(FROM + EAIIntegrations.class.getName() + WHERE_BASE_INTEGRATION_INTEGRATION_BASE_ID + baseId);
		List<EAIIntegrations> integrationsAssociated = integrationsAssociatedQuery.list();
		if (integrationsAssociated.size() >= 1)
			return;
		EAIIntegrationsBase base = session.get(EAIIntegrationsBase.class, baseId);
		session.delete(base);
	}

	private void deleteEventTime(final Integer id) {
		Query query = session.createQuery(
				FROM + EventTime.class.getName() + " where integration.integrationId =:integrationId");
		query.setParameter(INTEGRATION_ID, id);
		List<EventTime> eventTimes = query.list();
		if (eventTimes != null && !eventTimes.isEmpty()) {
			for (EventTime eventTime : eventTimes) {
				session.delete(eventTime);
			}
		}

	}

	/**
	 * To delete the criteria associated with the integration
	 * @param id : integration id
	 */
	private void deleteCriteria(final EAIIntegrations integrations) {
		List<EAICriteria> criteriaList = integrations.getEaiCriterias();

		if (criteriaList != null && !criteriaList.isEmpty()) {
			for (EAICriteria criteria : criteriaList) {
				session.delete(criteria);
			}
		}
	}

	/**
	 * to delete the unidirectional integration
	 * @param integrationId : id of the integration to be deleted
	 * @throws OpsHubBaseException
	 */
	private void deleteIntegration(final int integrationId, final boolean deleteGroup) throws OpsHubBaseException {
		Integer baseId = deleteUniDirectionalIntegration(integrationId);
		EAITimeboxEntityHistoryState.deleteHistoryState(session, baseId);
		deleteIntegrationBaseAndGroup(baseId, deleteGroup);
	}

	/**
	 * Delete create entity locks for given integration
	 * 
	 * @param integrationId
	 */
	private void deleteCreateLockForIntegration(final int integrationId) {

		// Create CriteriaBuilder
		CriteriaBuilder builder = session.getCriteriaBuilder();

		// Create CriteriaQuery
		CriteriaQuery<OIMEntityCreationLock> criteriaQuery = builder.createQuery(OIMEntityCreationLock.class);
		Root<OIMEntityCreationLock> root = criteriaQuery.from(OIMEntityCreationLock.class);

		// apply constraints
		criteriaQuery.select(root).where(builder.equal(root.get(OIMEntityCreationLock_.integration), integrationId));

		// execute query
		List<OIMEntityCreationLock> entityCreationLocks = session.createQuery(criteriaQuery).getResultList();
		for (OIMEntityCreationLock lock : entityCreationLocks) {
			session.delete(lock);
		}
	}

	/**
	 * This method deletes unidirectional integration and data in associated tables.
	 * It does not delete Integration base and Integration group associated with integrationId(EAIIntegrations)
	 * @param integrationId
	 * @return
	 * @throws OpsInternalError
	 * @throws ORMException
	 * @throws LoggerManagerException
	 */
	public Integer deleteUniDirectionalIntegration(final int integrationId)
			throws OpsInternalError, ORMException, LoggerManagerException {
		EAIIntegrations eaiIntegrations = session.get(EAIIntegrations.class, integrationId);

		deleteEventTime(integrationId);
		deleteCriteria(eaiIntegrations);

		Integer baseId = eaiIntegrations.getBaseIntegration().getIntegrationBaseId();
		Query query = session.createQuery(" from "+EaiEventProcessDefinition.class.getName()+" where integration.integrationId ="+integrationId);
		Iterator<EaiEventProcessDefinition> itr = query.list().iterator();
		while (itr.hasNext()) {
			EaiEventProcessDefinition pdef = itr.next();
			deleteAssociateLoggedEvent(pdef.getEventProcessDefinitionId());
			deleteAssociateProcessContext(pdef.getEventProcessDefinitionId());
			deleteAssociatedTransformationMapping(pdef.getEventProcessDefinitionId());
			deleteAssociatedReconciliations(integrationId);
			// rename integration log file
			renameLogFile(integrationId, LoggerManager.INTEGRATION);
			session.delete(pdef);
		}
		// delete sync info
		query = session.createQuery(" from "+OIMEntitySyncInfo.class.getName()+" where integration.integrationId ="+integrationId);
		List<OIMEntitySyncInfo> syncInfoList = query.list();
		for (OIMEntitySyncInfo syncInfo : syncInfoList) {
			session.delete(syncInfo);
		}
		// delete entity context
		query = session.createQuery(" from "+OIMEntityContext.class.getName()+" where integration.integrationId ="+integrationId);
		List<OIMEntityContext> entityContextList = query.list();
		for (OIMEntityContext entityContext : entityContextList) {
			session.delete(entityContext);
		}

		// delete create locks for integration
		deleteCreateLockForIntegration(integrationId);

		deleteSkippedEventsAssociated(integrationId);
		
		deleteProceessedUserDetails(integrationId);
		session.delete(eaiIntegrations);

		// first we need to make sure integration got deleted and then integration and delete job instance need to deleted
		// otherwise if there are job error on both integration and delete job then it won't be able to perform delete action.
		JobInstance jobInstance = eaiIntegrations.getSourceJobInstance();
		deleteAssociatedJobInstance(jobInstance, JobType.INTEGRATION);
		
		JobInstance deleteJobInstance = eaiIntegrations.getDeleteJobInstance();
		if(deleteJobInstance != null) {
			deleteAssociatedJobInstance(deleteJobInstance,JobType.DELETE);
		}
		return baseId;
	}
	
	private void deleteProceessedUserDetails(final int integrationId) {		
		String hql="delete from "+  EAIProcessedUser.class.getName() + WHERE_INTEGRATION_ID;
		org.hibernate.query.Query query = session.createQuery(hql);
		query.setParameter("id", integrationId);
		query.executeUpdate();
	}

	/**
	 * Deletes the skipped event entry for integration from EAIEventsSkipped
	 */
	private void deleteSkippedEventsAssociated(final Integer integrationId) {
		Query mappingQry = session.createQuery(FROM + EAIEventsSkipped.class.getName() + WHERE_INTEGRATION_ID);
		mappingQry.setParameter("id", integrationId);
		List<EAIEventsSkipped> events = mappingQry.getResultList();
		for (EAIEventsSkipped event : events) {
			session.delete(event);
		}
	}

	public void deleteAssociatedEntityInfo(final Integer systemId, final String projectId, final boolean deleteSource) {
		Query query = session.createQuery(" from " + OIMEntityInfo.class.getName() + " where entitySystem.systemId ="
				+ systemId + " and projectId.project_id='" + projectId + "'");
		Iterator<OIMEntityInfo> itr = query.list().iterator();
		while (itr.hasNext()) {
			OIMEntityInfo entityInfo = itr.next();
			deleteAssociatedLastUpdatedInfo(entityInfo);
			deleteAssociatedEntityHistoryState(entityInfo.getEntityInfoId());
			deleteAssociatedEntitySyncInfo(entityInfo.getEntityInfoId());
			deleteAssociatedCommentPolledState(entityInfo);
			deleteAssociatedReconcileContext(entityInfo.getEntityInfoId());
			deleteAssociatedHierarchyInfo(entityInfo);

			//will not delete source entity info. same source system is used in other migration, then will not be able to get globalId
			if(!deleteSource){
				session.delete(entityInfo);
			}
		}
	}

	private void deleteAssociatedReconcileContext(final Integer entityInfoId) {
		Criteria criteria = session.createCriteria(ReconcileContext.class)
				.add(Restrictions.eq("entityInfo.entityInfoId", entityInfoId));
		Iterator<ReconcileContext> itr = criteria.list().iterator();
		while (itr.hasNext()) {
			ReconcileContext entitySyncInfo = itr.next();
			session.delete(entitySyncInfo);
		}
	}

	private void deleteAssociatedHierarchyInfo(final OIMEntityInfo oimEntityInfo) {
		if (oimEntityInfo != null && oimEntityInfo.getHierarchyInfo() != null) {
			OIMEntityHierarchyInfo hierarchyInfo = oimEntityInfo.getHierarchyInfo();
			session.delete(hierarchyInfo);
		}
	}

	private void deleteAssociatedCommentPolledState(final OIMEntityInfo oimEntityInfo) {
		CriteriaBuilder builder = session.getCriteriaBuilder();
		CriteriaQuery<EAICommentPolledState> criteriaQuery = builder.createQuery(EAICommentPolledState.class);
		Root<EAICommentPolledState> root = criteriaQuery.from(EAICommentPolledState.class);
		criteriaQuery.select(root)
				.where(builder.equal(root.get(EAICommentPolledState_.entityInfo), oimEntityInfo.getEntityInfoId()));
		List<EAICommentPolledState> associatedCommentPolledStates = session.createQuery(criteriaQuery).getResultList();
		for (EAICommentPolledState associatedCommentPolled : associatedCommentPolledStates) {
			session.delete(associatedCommentPolled);
		}
	}

	private void deleteAssociatedLastUpdatedInfo(final OIMEntityInfo oimEntityInfo) {
		CriteriaBuilder builder = session.getCriteriaBuilder();
		CriteriaQuery<EntityLastUpdateTimeInfo> criteriaQuery = builder.createQuery(EntityLastUpdateTimeInfo.class);
		Root<EntityLastUpdateTimeInfo> root = criteriaQuery.from(EntityLastUpdateTimeInfo.class);
		criteriaQuery.select(root)
				.where(builder.equal(root.get(EntityLastUpdateTimeInfo_.oimEntityInfo),
						oimEntityInfo.getEntityInfoId()));
		List<EntityLastUpdateTimeInfo> associatedLastUpdateInfo = session.createQuery(criteriaQuery).getResultList();
		for (EntityLastUpdateTimeInfo lastUpdatedInfo : associatedLastUpdateInfo) {
			session.delete(lastUpdatedInfo);
		}
	}

	private void deleteAssociatedEntitySyncInfo(final Integer entityInfoId) {
		Criteria criteria = null;
		criteria=session.createCriteria(OIMEntitySyncInfo.class).add(Restrictions.eq("entityInfoId.entityInfoId", entityInfoId));
		Iterator<OIMEntitySyncInfo> itr = criteria.list().iterator();
		while (itr.hasNext()) {
			OIMEntitySyncInfo entitySyncInfo = itr.next();
			session.delete(entitySyncInfo);
		}
	}

	private void deleteAssociatedEntityHistoryState(final Integer entityInfoId) {
		Criteria criteria = null;
		criteria=session.createCriteria(OIMEntityHistoryState.class).add(Restrictions.eq("entityInfo.entityInfoId", entityInfoId));
		Iterator<OIMEntityHistoryState> itr = criteria.list().iterator();
		while (itr.hasNext()) {
			OIMEntityHistoryState entityHistoryState = itr.next();
			session.delete(entityHistoryState);
		}
	}

	/**
	 * To delete post failure notification associated with the integration
	 * @param integrationId : id of the integration to be deleted
	 */
	private void deletePostFailureNotification(final int integrationId) {
		Query query = session.createQuery(
				" from " + EaiNotificationIntegration.class.getName() + " where integGroup.groupId =" + integrationId);
		List<EaiNotificationIntegration> notifications = query.list();
		if (notifications.size() > 0) {
			for (EaiNotificationIntegration notificationObj : notifications) {
				Iterator<EaiNotificationParams> eaiNotificationParams = notificationObj.getNotificationParams().iterator();
				while (eaiNotificationParams.hasNext()) {
					EaiNotificationParams notificationParams = eaiNotificationParams.next();
					session.delete(notificationParams);
				}
				session.delete(notificationObj);
			}
		}
	}

	/**
	 * To delete associated event transformation mapping
	 * @param eventProcessDefinitionId : event process definition id
	 */
	private void deleteAssociatedTransformationMapping(
			final int eventProcessDefinitionId) {
		deleteTransMapping(session, eventProcessDefinitionId);
	}

	/**
	 * To delete Associated job instance
	 * @param jobInstance : JobInstance object
	 */
	private void deleteAssociatedJobInstance(final JobInstance jobInstance, final JobType integrationOrReco) {
		deleteAssociatedJobFailedEvent(jobInstance.getJobInstanceId());
		if (JobType.INTEGRATION == integrationOrReco) {
			deleteEventTime(jobInstance);
		}
		JobsBO.deleteFromDB(session, jobInstance);
	}

	/**
	 * To delete associated event time
	 * @param jobInstance : Object of JObInstance
	 */
	private void deleteEventTime(final JobInstance jobInstance) {
		Query query = session.createQuery(" from " + EventTime.class.getName() + " where integration.sourceJobInstance.jobInstanceId="+ jobInstance.getJobInstanceId());
		Iterator<EventTime> eventTimeItr = query.list().iterator();
		while (eventTimeItr.hasNext()) {
			EventTime eventTime = eventTimeItr.next();
			session.delete(eventTime);
		}
	}

	private void deleteEventTime(final EAIReconciliations eaiReco) {

		Query query = session.createQuery(" from " + EventTime.class.getName() + " where reconciliation.reconciliationId=:recoId");
		query.setParameter("recoId", eaiReco.getReconciliationId());
		Iterator<EventTime> eventTimeItr = query.list().iterator();
		while (eventTimeItr.hasNext()) {
			EventTime eventTime = eventTimeItr.next();
			session.delete(eventTime);
		}
	}

	/**
	 * To delete associated process context parameters
	 * @param eventProcessDefinitionId : event process definition id
	 */
	private void deleteAssociateProcessContext(final int eventProcessDefinitionId) {
		Query qry = null;
		qry = session.createQuery(FROM + EAIProcessContext.class.getName() +  " where eventProcessDefinition.eventProcessDefinitionId=:eventProcessDefId");
		qry.setParameter("eventProcessDefId", eventProcessDefinitionId);
		Iterator<EAIProcessContext> itr = qry.list().iterator();
		while (itr.hasNext()) {
			EAIProcessContext processContext = itr.next();
			session.delete(processContext);
		}
	}

	/**
	 * To delete the logged events associated with the integration
	 * @param eventProcessDefinitionId : event process definition id
	 */
	private void deleteAssociateLoggedEvent(final int eventProcessDefinitionId) {
		Query qry = null;
		qry = session.createQuery(FROM + EaiEventsLogged.class.getName() +  " where eventProcessDefinition.eventProcessDefinitionId=:eventProcessDefId");
		qry.setParameter("eventProcessDefId", eventProcessDefinitionId);
		Iterator<EaiEventsLogged> itr = qry.list().iterator();
		while (itr.hasNext()) {
			EaiEventsLogged loggedEvent = itr.next();
			deleteAssociateSrcFailed(loggedEvent.getId());
			session.delete(loggedEvent);
		}
	}

	/**
	 * To delete the logged events associated
	 * @param eventProcessDefinitionId : event process definition id
	 */
	private void deleteAssociatedJobFailedEvent(final int jobInstanceId) {
		Query qry = null;
		qry = session.createQuery(FROM + EaiEventsLogged.class.getName() +  " where sourceJobInstance.jobInstanceId=:jobInstanceId");
		qry.setParameter(PARAM_JOB_INSTANCE, jobInstanceId);
		Iterator<EaiEventsLogged> itr = qry.list().iterator();
		while (itr.hasNext()) {
			EaiEventsLogged loggedEvent = itr.next();
			deleteAssociateSrcFailed(loggedEvent.getId());
			session.delete(loggedEvent);
		}
	}

	private void deleteAssociateSrcFailed(final Integer eventsLoggedId) {
		Criteria criteria = null;
		criteria=session.createCriteria(SourceFailed.class).add(Restrictions.eq("eaiEventsLogged.id", eventsLoggedId));
		Iterator<SourceFailed> itr = criteria.list().iterator();
		while (itr.hasNext()) {
			SourceFailed srcFailed = itr.next();
			session.delete(srcFailed);
		}
	}

	/**
	 * To delete the reconciliations associated with the integration
	 * @param integrationId
	 * @throws OpsInternalError
	 * @throws LoggerManagerException
	 * @throws ORMException
	 */
	private void deleteAssociatedReconciliations(final int integrationId) throws OpsInternalError, ORMException, LoggerManagerException {
		Query qry = null;
		qry = session.createQuery(FROM + EAIReconciliations.class.getName() +  " where eaiIntegrations.integrationId=:integrationId");
		qry.setParameter(INTEGRATION_ID, integrationId);
		Iterator<EAIReconciliations> itr = qry.list().iterator();
		while (itr.hasNext()) {
			EAIReconciliations eaiReconciliation = itr.next();
			Query jobStatusQuery = session.createQuery(FROM+ JobInstance.class.getName() + " where jobInstanceId=:jobInstanceId");
			jobStatusQuery.setParameter(PARAM_JOB_INSTANCE, eaiReconciliation.getReconcileJobInstance().getJobInstanceId());
			Iterator<JobInstance> jobInstanceitr = jobStatusQuery.list().iterator();
			while (jobInstanceitr.hasNext()) {
				JobInstance jobInstance = jobInstanceitr.next();
				if (JobStatus.ACTIVE.equals(jobInstance.getStatus().getId())) {
					OpsHubLoggingUtil.error(LOGGER,"Could not delete the selected Integration(s), since Reconciliation(s) associated with them are in Active State", null);
					throw new OpsInternalError(AdminUIConstants.ERROR_CODE_003596, new String[] {}, null);
				}
				Query reconContext =session.createQuery(FROM + ReconcileContext.class.getName() +  " where jobInstance.jobInstanceId=:jobInstanceId");
				reconContext.setParameter(PARAM_JOB_INSTANCE, jobInstance.getJobInstanceId());
				for (ReconcileContext recCtx : (List<ReconcileContext>) reconContext.list()) {
					session.delete(recCtx);
				}
				// rename related log file
				renameLogFile(eaiReconciliation.getReconciliationId(), LoggerManager.RECONCILIATION);
				session.delete(eaiReconciliation);
				deleteAssociatedJobInstance(jobInstance, JobType.RECO);
				deleteEventTime(eaiReconciliation);
			}
		}
	}

	/**
	 * To delete the reconciliation based on reconciliationIds
	 * 
	 * @param reconciliationId
	 * @throws OpsHubBaseException
	 */
	public List<String> deleteReconciliations(final List<String> reconciliationIds) throws OpsHubBaseException {
		Query qry = null;
		List<String> listDeletedReconciliations = new ArrayList<>();
		List<String> listActiveReconciliations = new ArrayList<>();
		Iterator<String> listIterator = reconciliationIds.iterator();

		while (listIterator.hasNext()) {
			String reconciliationId = listIterator.next();
			qry = session.createQuery(FROM + EAIReconciliations.class.getName() + " where reconciliationId=:reconId");
			qry.setParameter("reconId", Integer.parseInt(reconciliationId));
			EAIReconciliations eaiReconciliation = (EAIReconciliations) qry.uniqueResult();

			if(eaiReconciliation==null) continue;

			JobInstance jobInstance = eaiReconciliation.getReconcileJobInstance();
			if (JobStatus.ACTIVE.equals(jobInstance.getStatus().getId())) {
				listActiveReconciliations.add(reconciliationId);
				OpsHubLoggingUtil.error(LOGGER,"Could not delete the selected Reconciliation(s), since they are in Active State", null);
				continue;
			}
			listDeletedReconciliations.add(reconciliationId);
			Query reconContext =session.createQuery(FROM + ReconcileContext.class.getName() +  " where jobInstance.jobInstanceId=:jobInstanceId");
			reconContext.setParameter(PARAM_JOB_INSTANCE, jobInstance.getJobInstanceId());
			for (ReconcileContext recCtx : (List<ReconcileContext>) reconContext.list()) {
				session.delete(recCtx);
			}
			// rename related log file
			renameLogFile(eaiReconciliation.getReconciliationId(), LoggerManager.RECONCILIATION);
			deleteEventTime(eaiReconciliation);
			session.delete(eaiReconciliation);
			deleteAssociatedJobInstance(jobInstance, JobType.RECO);
			setIntegrationMode(session, eaiReconciliation.getEaiIntegrations(), ConfigMode.INTEGRATION.getModeId());

		}

		return listActiveReconciliations;
	}

	public void throwErrorIfMappingInActiveIntegration(final Integer mappingId) throws OpsHubBaseException{										
		List<Integer> mappingIdList = Arrays.asList(new Integer[]{mappingId});
		List<IntegrationStatusInfo> activeIntegrations = getActiveIntegrations(session, AdminConstants.EAI_MAPPING_CE, mappingIdList);
		//Throwing error if the mapping has any active associated integrations. 
		if(activeIntegrations != null && !activeIntegrations.isEmpty()){
			StringBuilder combinedActiveIntegrationNames = new StringBuilder("["); 
			for(int i=0;i<activeIntegrations.size()-1;i++){
				combinedActiveIntegrationNames.append(activeIntegrations.get(i).getIntegrationName()).append(", ");
			}
			combinedActiveIntegrationNames.append(activeIntegrations.get(activeIntegrations.size()-1).getIntegrationName());
			combinedActiveIntegrationNames.append("]");
			throw new OpsHubBaseException("017851", new String[] { combinedActiveIntegrationNames.toString() }, null);
		}		
	}

	/**
	 * @param session:
	 *            Hibernate session
	 * @param entityName
	 *            : Name of the entity: workflow/mapping
	 * @param entityIds
	 *            : list of selected workflow/mapping ids
	 * @return
	 */
	public List<IntegrationStatusInfo> getActiveIntegrations(final Session session, final String entityName,
			final List<Integer> entityIds) {
		List<IntegrationStatusInfo> integrationList = new ArrayList<>();
		String getIntegrationsAssociatedQuery = null;
		if (entityName.equals(AdminConstants.EAI_WORKFLOW_CE)) {
			getIntegrationsAssociatedQuery = " select distinct integration.baseIntegration.integrationBaseId, integration.baseIntegration.integrationGroup.groupName from "
					+ EaiEventProcessDefinition.class.getName()
					+ " where eaiprocessDefinition.processDefinitionId IN(:ids) and integration.status.id="
					+ EAIIntegrationStatus.ACTIVE;
		} else if (entityName.equals(AdminConstants.EAI_MAPPING_CE)) {
			getIntegrationsAssociatedQuery = " select distinct eventProcessDefinition.integration.baseIntegration.integrationBaseId, eventProcessDefinition.integration.baseIntegration.integrationGroup.groupName from "
					+ EaiEventTransformationMapping.class.getName()
					+ " where transformationMapping.id IN(:ids) and eventProcessDefinition.integration.status.id="
					+ EAIIntegrationStatus.ACTIVE;
		}
		Query query = session.createQuery(getIntegrationsAssociatedQuery);
		query.setParameterList("ids", entityIds);
		List<Object[]> queryResponse = query.list();
		for (Object[] response : queryResponse) {
			IntegrationStatusInfo integrationInfo = new IntegrationStatusInfo();
			integrationInfo.setIntegrationId((Integer) response[0]);
			integrationInfo.setIntegrationName(String.valueOf(response[1]));
			integrationList.add(integrationInfo);
		}
		return integrationList;
	}

	/**
	 * This method is used to get the list of integration names to show in  message while editing fieldmapping/workflow
	 * it checks whether there is any active integration on the selected workflow/field mapping
	 * @param session : Hibernate session
	 * @param entityName : Name of the entity
	 * @param pkey : id of selected workflow/field mapping
	 * @return list of integration names
	 */
	public List<String> getActiveIntegrationsAssociated(final Session session, final String entityName, int pkey) {
		String getIntegrationsAssociated = null;
		String getReconciliationsAssociated = null;
		String queryForMappingAssociatedWithReconciliation = null;
		if (entityName.equals(AdminConstants.EAI_WORKFLOW_CE)) {
			getIntegrationsAssociated = " select distinct integration.baseIntegration.integrationName from "+EaiEventProcessDefinition.class.getName()+" where eaiprocessDefinition.processDefinitionId =:id and integration.status.id="+EAIIntegrationStatus.ACTIVE;
			getReconciliationsAssociated = "select distinct reconciliationName from " + EAIReconciliations.class.getName() + " where eaiProcessDefiniton.processDefinitionId=:id and reconcileJobInstance.status.id="+JobStatus.ACTIVE; 
		} else if (entityName.equals(AdminConstants.EAI_MAPPING_CE)) {
			pkey = EAIMapperIntermidiateXML.getEaiMappingXML(session, pkey).getId();
			getIntegrationsAssociated = " select distinct eventProcessDefinition.integration.baseIntegration.integrationName from "+EaiEventTransformationMapping.class.getName()+" where transformationMapping.id =:id and eventProcessDefinition.integration.status.id="+EAIIntegrationStatus.ACTIVE;
			queryForMappingAssociatedWithReconciliation = "select distinct reconciliationName from " + EAIReconciliations.class.getName()
					+ " where eaiIntegrations.baseIntegration.integrationName IN(select distinct eventProcessDefinition.integration.baseIntegration.integrationName from "
					+ EaiEventTransformationMapping.class.getName()
					+ " where transformationMapping.id =:id) and reconcileJobInstance.status.id=" + JobStatus.ACTIVE;
		}
		Query query = session.createQuery(getIntegrationsAssociated);
		query.setParameter("id", pkey);

		List lst = query.list();
		if (getReconciliationsAssociated != null) {
			Query reconciliationQuery = session.createQuery(getReconciliationsAssociated);
			reconciliationQuery.setParameter("id", pkey);
			List reconcileList = reconciliationQuery.list();
			lst.addAll(reconcileList);
		}
		if (queryForMappingAssociatedWithReconciliation != null) {
			Query reconciliationQuery = session.createQuery(queryForMappingAssociatedWithReconciliation);
			reconciliationQuery.setParameter("id", pkey);
			List reconcileList = reconciliationQuery.list();
			lst.addAll(reconcileList);
		}
		return lst;

	}

	/**
	 * This method is used to get the filecontent for workflow or fieldmapping
	 * @param session : Hibernate session
	 * @param entityName : Based on it we decide which entity we have to use like workflow or field mapping
	 * @param pkey : id of workflow or field mapping
	 * @return file content and file name
	 */
	public HashMap<String, String> getContent(final Session session, final String entityName, final int pkey) {
		HashMap<String, String> file = new HashMap<String, String>();
		if (entityName.equals(AdminConstants.EAI_WORKFLOW_CE)) {
			EaiProcessDefinition processDefObj = session.get(EaiProcessDefinition.class, pkey);
			file.put(AdminConstants.FILE_NAME, processDefObj.getProcessDefinitionName());
			file.put(AdminConstants.FILE_CONTENT, processDefObj.getProcessDefinitionXml());
		} else if (entityName.equals(AdminConstants.EAI_MAPPING_CE)) {
			Scripts transformationScript = session.get(Scripts.class, pkey);
			file.put(AdminConstants.FILE_NAME, transformationScript.getScriptName());
			// In View Content show mapping and reconcile combined data in xml format.
			file.put(AdminConstants.FILE_CONTENT, MapperUtility.getMappingContents(transformationScript));
		}
		return file;
	}

	public HashMap<String, List<Map<String, Object>>> getAdvanceConfigurationData(final Systems sourceSystem,
			final Systems destinationSystem, final Integer integrationId, final boolean isClone,
			final HashMap<String, Object> sourceProcessContext, final HashMap<String, Object> destProcessContext)
			throws OpsHubBaseException {
		return getAdvanceConfigurationData(sourceSystem, destinationSystem, integrationId, isClone,
				sourceProcessContext, destProcessContext, true);
	}

	/**
	 * To get the advance configuration data
	 * 
	 * @param sourceSystem
	 *            : source system id
	 * @param destinationSystem
	 *            : destination system id
	 * @param integrationId
	 *            : integration id
	 * @param includeFieldsMeta
	 * @return : form of advance configuration
	 * @throws OpsHubBaseException
	 */
	public HashMap<String,List<Map<String, Object>>> getAdvanceConfigurationData(
			final Systems sourceSystem, final Systems destinationSystem,
			Integer integrationId, final boolean isClone,
			final HashMap<String, Object> sourceProcessContext,
			final HashMap<String, Object> destProcessContext, final boolean includeFieldsMeta) throws OpsHubBaseException {
		HashMap<String, List<Map<String, Object>>> advanceDataMap = new HashMap<String, List<Map<String, Object>>>();

		/**
		 * Create adapter object and
		 * fetch fields metadata for both source and destination system.
		 * fetch all custom fields for both source and destination 
		 */
		Map<String, String> destCustomfield = new HashMap<>();
		Map<String, String> destLookupField = new HashMap<>();
		Map<String, String> sourceCustomField = new HashMap<>();
		Map<String, String> sourceFieldsInclBooleans = new HashMap<>();
		Map<String, String> destCommentfields = new HashMap<>();
		Map<String, String> sourceCommentFields = new HashMap<>();

		Map<String, String> sourceDataToTrace = new HashMap<String, String>();

		if (includeFieldsMeta
				&& !sourceSystem.getSystemType().getParentSystemType().getSystemTypeId().equals(SystemType.SCM)) {
			

			// jiraworkflow is not needed for fetching mapper feildsmetadata
			destProcessContext.remove(UIConstants.JIRA_WORKFLOW);

			// Check for minimum fields in destination Process Context.
			List<String> mandatoryKeys = getMandatoryJobOrProcessContextKeys(session, destinationSystem.getSystemId(),
					integrationId, false, false);
			// Flag for all the mandatory existence keys
			boolean isExist = true;
			// If all mandatory destination context is not available, we will
			// not be able to load fields meta, hence check if all mandatory
			// context values are available.
			for (Iterator<String> iterator = mandatoryKeys.iterator(); iterator.hasNext();) {
				String keyName = iterator.next();
				if (!destProcessContext.containsKey(keyName)) {
					isExist = false;
					break;
				}
			}
			EndSystemFields ep1Fields = getEndPointsFields(isExist, sourceSystem, sourceProcessContext,
					true);
			EndSystemFields ep2Fields = getEndPointsFields(isExist, destinationSystem,
					destProcessContext, false);

			sourceCustomField = ep1Fields.getCustomField();
			sourceFieldsInclBooleans = ep1Fields.getFieldsInclBooleans();
			sourceCommentFields = ep1Fields.getCommentFields();
			sourceDataToTrace = getMapOfDataToTraceList(sourceSystem, sourceProcessContext);

			destCustomfield = ep2Fields.getCustomField();
			destLookupField = ep2Fields.getLookUpFields();
			destCommentfields = ep2Fields.getCommentFields();
		}

		Integer jobId = null;
		EAIIntegrations eaiIntegration = null;
		JobInstance jobInstance = null;
		HashMap<String, FieldValue> jobInitialFieldVals = new HashMap<String, FieldValue>();
		if (integrationId == -1)
			jobId = -1;
		else {
			eaiIntegration = session.get(EAIIntegrations.class, integrationId);
			jobId = eaiIntegration.getSourceJobInstance().getJobInstanceId();
			jobInstance = session.get(JobInstance.class, jobId);
			integrationId = eaiIntegration.getIntegrationId();
			setJobContextIntialValues(jobId, jobInitialFieldVals, sourceSystem, integrationId, isClone);
		}

		List<Map<String, Object>> srcJobContext = getMandatoryOrNonMandatoryFieldOnly(
				getJobContextForm(sourceSystem, integrationId, jobId != -1, jobInitialFieldVals,
						jobInstance),
				false);
		updateJobContextForEventDetectionInputs(srcJobContext, sourceCustomField, jobInitialFieldVals,
				sourceDataToTrace);

		Collections.sort(srcJobContext, mapComparator);

		advanceDataMap.put(UIConstants.JOB_CONTEXT, srcJobContext);
		List<Map<String, Object>> srcProcessCtxt = (List<Map<String, Object>>) getProcessContextParameters(session,
				sourceSystem, false, integrationId, true, isClone, sourceCustomField)
						.get(UIConstants.PROCESS_CONTEXT);
		skipInvalidSrcProcessContext(srcProcessCtxt);
		// Hide source process context.
		advanceDataMap.put(UIConstants.PROCESS_CONTEXT,
				getSourceProcessContextUiMeta((ArrayList<Map<String, Object>>) srcProcessCtxt));

		Integer companyId = sourceSystem.getCompany().getCompanyId();

		List<Entities> entityList = getIntegrationContextEntities(sourceSystem);
		List<String> entityNameList = new ArrayList<>();
		for (Entities entity : entityList) {
			entityNameList.add(entity.getName());
		}
		int criteriaEntityIndex = entityNameList.indexOf(UIConstants.CRITERIA_ENTITY);
		int criteriaStorageEntityIndex = entityNameList.indexOf(UIConstants.CRITERIA_DATA_STORAGE);
		if (criteriaStorageEntityIndex == -1) {
			criteriaStorageEntityIndex = entityNameList.indexOf(UIConstants.CRITERIA_DATA_STORAGE_WITH_NO_STORAGE_OPTION);
		}
		if (criteriaEntityIndex > 0 && criteriaStorageEntityIndex > 0 && entityList.size() > criteriaEntityIndex + 1) {
			Collections.swap(entityList, criteriaStorageEntityIndex, criteriaEntityIndex + 1);
		}

		HashMap<String, FieldValue> otherConfigInitialFieldVals = new HashMap<String, FieldValue>();
		Map<String, FieldValue> criteriaIntialValMap = getCriteriaIntialVal(session, sourceSystem,
				jobId);
		if (criteriaIntialValMap != null) {
			otherConfigInitialFieldVals.putAll(criteriaIntialValMap);
		}
		Map<String, FieldValue> integrationCtxValMap = getOIMContext(session, eaiIntegration);
		if (integrationCtxValMap != null) {
			otherConfigInitialFieldVals.putAll(integrationCtxValMap);
		} else
			otherConfigInitialFieldVals.put(LOCK, new FieldValue(null, UIConstants.VALUE_YES, null));

		FormLoader formLoader = new FormLoader(session, companyId);

		Map<Integer, String> pollingType = getPollingType();

		Map<Integer, String> globalSchedule = new HashMap<Integer, String>();
		globalSchedule.put(JobSchedule.GLOBAL_SCHEDULE_ID, JobSchedule.GLOBAL_SCHEDULE_NAME);
		FieldValue scheduleFieldValue = new FieldValue(null, null, globalSchedule);

		FieldValue globalScheduleConfigured = jobInitialFieldVals.get(JobSchedule.IS_GLOBAL_SCHEDULE_CONFIGURED);
		if (integrationId == -1) {
			otherConfigInitialFieldVals.put(LOCK, new FieldValue(null, UIConstants.VALUE_YES, null));
			scheduleFieldValue.setValue(JobSchedule.GLOBAL_SCHEDULE_ID);
			otherConfigInitialFieldVals.put(UIConstants.POLLING_TYPE, new FieldValue(null, EAIPollingType.BOTH_EVENTS, pollingType));
			otherConfigInitialFieldVals.put(UIConstants.CONFIGURE_CRITERIA, new FieldValue(null, UIConstants.FALSE_VAL, null));
		} else {
			if (globalScheduleConfigured != null && globalScheduleConfigured.getValue() != null
					&& Boolean.TRUE.toString().equalsIgnoreCase(String.valueOf(globalScheduleConfigured.getValue()))) {
				scheduleFieldValue.setValue(JobSchedule.GLOBAL_SCHEDULE_ID);
			}
			otherConfigInitialFieldVals.put(UIConstants.POLLING_TYPE, new FieldValue(null, null, pollingType));
		}
		otherConfigInitialFieldVals.put(UIConstants.SCHEDULE_NAME, scheduleFieldValue);
		if (otherConfigInitialFieldVals.containsKey(UIConstants.SYNCFIELDNAME))
			otherConfigInitialFieldVals.put(UIConstants.SYNCFIELDNAME, new FieldValue(null, otherConfigInitialFieldVals.get(UIConstants.SYNCFIELDNAME).getValue(), (Map)sourceCustomField));	
		else
			otherConfigInitialFieldVals.put(UIConstants.SYNCFIELDNAME, new FieldValue(null, null, (Map)sourceCustomField));

		if (entityNameList.contains(UIConstants.CRITERIA_DATA_STORAGE)
				|| entityNameList.contains(UIConstants.CRITERIA_DATA_STORAGE_WITH_NO_STORAGE_OPTION)) {
			// If system supports criteria storage type selection, load values
			Object storageTypeValue = null, storageFieldNameValue = null;
			if (jobInitialFieldVals.containsKey(UIConstants.CRITERIA_STORAGE_TYPE_KEY)
					&& jobInitialFieldVals.get(UIConstants.CRITERIA_STORAGE_TYPE_KEY).getValue() != null) {
				storageTypeValue = jobInitialFieldVals.get(UIConstants.CRITERIA_STORAGE_TYPE_KEY).getValue();
				if (storageTypeValue.toString() == UIConstants.CRITERIA_TYPE_NOT_CONFIGURED)
					storageTypeValue = null;
			} else {
				storageTypeValue = UIConstants.CRITERIA_TYPE_IN_DB;
			}
			if (jobInitialFieldVals.containsKey(UIConstants.CRITERIA_STORAGE_FIELD_KEY)
					&& jobInitialFieldVals.get(UIConstants.CRITERIA_STORAGE_FIELD_KEY).getValue() != null) {
				storageFieldNameValue = jobInitialFieldVals.get(UIConstants.CRITERIA_STORAGE_FIELD_KEY).getValue();
				if (storageFieldNameValue.toString().isEmpty())
					storageFieldNameValue = null;
			}
			otherConfigInitialFieldVals.put(UIConstants.CRITERIA_STORAGE_TYPE_KEY, new FieldValue(null, storageTypeValue, null));
			otherConfigInitialFieldVals.put(UIConstants.CRITERIA_STORAGE_FIELD_KEY, new FieldValue(null, storageFieldNameValue,
					(Map) sourceFieldsInclBooleans));
		}
		
		//compute delete job advance config parameters.
		if(eaiIntegration!=null) {
			JobInstance deleteJob = eaiIntegration.getDeleteJobInstance();
			if(deleteJob!=null) {
				otherConfigInitialFieldVals.put(Constants.DETECT_SOURCE_DELETE_FIELD , new FieldValue(null, Constants.ENABLED_DETECT_SOURCE_DELETE, null));
				otherConfigInitialFieldVals.put(Constants.DELETE_JOB_SCHEDULE, new FieldValue(null, String.valueOf(deleteJob.getSchedule().getId()), null));
			
				Map<String,String> deleteJobContext = EaiUtility.getJobContextValues(session, deleteJob.getJobInstanceId());
				if(deleteJobContext != null && deleteJobContext.get(Constants.DELETE_JOB_POLLING_DATE)!=null) {
					otherConfigInitialFieldVals.put(Constants.DELETE_JOB_POLLING_TYPE, new FieldValue(null, Constants.DeleteJobPollingType.POLLING_BY_DATE, null));
					otherConfigInitialFieldVals.put(Constants.DELETE_JOB_POLLING_DATE, new FieldValue(null, deleteJobContext.get(Constants.DELETE_JOB_POLLING_DATE), null));
				} else {
					otherConfigInitialFieldVals.put(Constants.DELETE_JOB_POLLING_TYPE, new FieldValue(null, Constants.DeleteJobPollingType.COMPLETE_POLLING, null));
				}
			}
		}

		List<Map<String, Object>> advanceConfigData = getIntegrationContextForm(integrationId, entityList,
				otherConfigInitialFieldVals, formLoader);

		advanceDataMap.put(UIConstants.ADVANCE_CONFIGURATIOn,
				updateFormValues((ArrayList<Map<String, Object>>) advanceConfigData, integrationId, jobInstance, sourceSystem, true));

		if (destinationSystem != null) {
			advanceDataMap.put(UIConstants.DEST_PROCESS_CONTEXT,
					(List<Map<String, Object>>) getProcessContextParameters(session, destinationSystem,
							false,
							integrationId, false, isClone, destCustomfield).get(UIConstants.PROCESS_CONTEXT));
			// Bug: 61243 - Adding new field in UI when Jama acts as
			// destination.
			if (SystemType.JAMA.equals(destinationSystem.getSystemType().getSystemTypeId())) {
				if (!destLookupField.isEmpty()) {
					List<Map<String, Object>> destProcessContextLocal = advanceDataMap.get(UIConstants.DEST_PROCESS_CONTEXT);
					for (Map<String, Object> destContextMap : destProcessContextLocal) {
						if (destContextMap.get(UIConstants.KEY) != null
								&& EAIJobConstants.JAMA.JAMA_STATE_TRANSITION_FIELD.equals(destContextMap.get(UIConstants.KEY))) {
							destContextMap.put(UIConstants.LOOKUP, destLookupField);
						}
					}
				}
			}
		}

		if (sourceSystem.getSystemType().getParentSystemType().getSystemTypeId().equals(SystemType.SCM)) {

			// remove link field from advance map destination process context
			// for
			// destination system .
			List<Map<String, Object>> listDestContext = advanceDataMap.get(UIConstants.DEST_PROCESS_CONTEXT);

			advanceDataMap.put(UIConstants.DEST_PROCESS_CONTEXT,
					getListWithoutProperty(listDestContext, UIConstants.LINKFIELDNAME));

			List<Map<String, Object>> listAdvConfig = advanceDataMap.get(UIConstants.ADVANCE_CONFIGURATIOn);
			// remove sync confirmation feild in case of scm system
			advanceDataMap.put(UIConstants.ADVANCE_CONFIGURATIOn,
					getListWithoutProperty(listAdvConfig, UIConstants.SYNCFIELDNAME));

		}

		// comment fields LookUp set Destination Process Context
		if (!destCommentfields.isEmpty()) {
			List<Map<String, Object>> listDestContext = advanceDataMap.get(UIConstants.DEST_PROCESS_CONTEXT);
			changePTCCommentLookUp(destCommentfields, listDestContext);
		}
		// comment fields LookUp set for Job Context
		if (!sourceCommentFields.isEmpty()) {
			List<Map<String, Object>> listJobContext = advanceDataMap.get(UIConstants.JOB_CONTEXT);
			changePTCCommentLookUp(sourceCommentFields, listJobContext);
		}
		/*
		 * If request is for clone make the fields editable and enable for form
		 * fields
		 */
		if (isClone) {
			makeAllFieldEditable(advanceDataMap);
		}
		return advanceDataMap;
	}

	/**
	 * Return the data to trace support by the connector for the event detection
	 * feature. If connector does not support then return empty map.
	 * 
	 * @param sourceSystem
	 * @param sourceProcessContext
	 * @return
	 * @throws OIMMapperException
	 */
	private Map<String, String> getMapOfDataToTraceList(final Systems sourceSystem,
			final HashMap<String, Object> sourceProcessContext) throws OIMMapperException {
		MapperBO mapperBO = new MapperBO(this.session);
		return mapperBO.getListOfDataTraceForEventDetection(sourceSystem.getSystemId(), sourceProcessContext);
	}

	/**
	 * Replace the jobContext for the eventDetection inputs.
	 * 
	 * @param srcJobContext
	 * @param sourceCustomField
	 * @param jobInitialFieldVals
	 */
	private void updateJobContextForEventDetectionInputs(final List<Map<String, Object>> srcJobContext,
			final Map<String, String> sourceCustomField, final HashMap<String, FieldValue> jobInitialFieldVals,
			final Map<String, String> sourceDataToTrace) {
		if (CollectionUtils.isNotEmpty(srcJobContext)) {
			for (Map<String, Object> jobContext : srcJobContext) {
				String jobContextKey = (String) jobContext.get(UIConstants.KEY);
				replaceTheFormValueAndLookup(UIConstants.FIELD_TO_UPDATE, jobContext, jobContextKey, sourceCustomField,
						jobInitialFieldVals);
				replaceTheFormValueAndLookup(UIConstants.DATA_TO_TRACE, jobContext, jobContextKey, sourceDataToTrace,
						jobInitialFieldVals);
			}
		}
	}

	/**
	 * Replace the jobContext for value and its lookup.
	 * 
	 * @param fieldtoCheck
	 * @param jobContext
	 * @param jobContextKey
	 * @param lookupValues
	 * @param jobInitialFieldVals
	 */
	private void replaceTheFormValueAndLookup(final String fieldtoCheck, final Map<String, Object> jobContext,
			final String jobContextKey, final Map<String, String> lookupValues,
			final HashMap<String, FieldValue> jobInitialFieldVals) {
		if (fieldtoCheck.equals(jobContextKey)) {
			if (jobInitialFieldVals.containsKey(jobContextKey)) {
				jobContext.put(UIConstants.VALUE, jobInitialFieldVals.get(jobContextKey));
			}
			jobContext.put(UIConstants.LOOKUP, lookupValues);
		}
	}

	private EndSystemFields getEndPointsFields(final boolean isExist, final Systems ep1System,
			final Map<String, Object> processContext, final boolean isEp1) throws OIMMapperException {
		MapperBO mapperBo = new MapperBO(session);
		Map<String, String> customField = new HashMap<String, String>();
		Map<String, String> fieldsInclBooleans = new HashMap<String, String>();
		Map<String, String> commentFields = new HashMap<String, String>();
		Map<String, String> lookUpFields = new HashMap<String, String>();
		Map<String, String> customfieldsDisplay = new HashMap<String, String>();

		List<FieldsMeta> fieldMetadata = mapperBo.getFieldsMetadata(ep1System.getSystemId(), processContext, false,
				false);
		if(!isExist && !isEp1)
			fieldMetadata = Collections.EMPTY_LIST;
		for (Iterator iterator = fieldMetadata.iterator(); iterator.hasNext();) {
			FieldsMeta fieldsMeta = (FieldsMeta) iterator.next();
			if (fieldsMeta.getDataType().equals(DataType.TEXT)|| fieldsMeta.getDataType().equals(DataType.HTML)
				|| fieldsMeta.getDataType().equals(DataType.HYPERLINK)|| fieldsMeta.getDataType().equals(DataType.RTF)|| fieldsMeta.getDataType().equals(DataType.WIKI) || fieldsMeta.getDataType().equals(DataType.MARKDOWN)) {
				customField.put(fieldsMeta.getInternalName(), fieldsMeta.getDisplayName());
				customfieldsDisplay.put(fieldsMeta.getDisplayName(), fieldsMeta.getDisplayName());
			}
			else if (fieldsMeta.getDataType().equals(DataType.BOOLEAN)) {
				fieldsInclBooleans.put(fieldsMeta.getInternalName(), fieldsMeta.getDisplayName());
			}
			else if (fieldsMeta.getDataType().equals(DataType.LOOKUP)) {
				lookUpFields.put(fieldsMeta.getInternalName(), fieldsMeta.getDisplayName());
			}
			if (ep1System.getSystemType().getSystemTypeId().equals(SystemType.PTC) &&fieldsMeta.getSystemNativeDataType() != null
					&& fieldsMeta.getSystemNativeDataType().equals(PTCConstants.PTC_COMMENT_FIELD_TYPE)) {
				commentFields.put(fieldsMeta.getInternalName(), fieldsMeta.getDisplayName());
			}
		}
		fieldsInclBooleans.putAll(customField);

		return new EndSystemFields(customField, fieldsInclBooleans, commentFields, lookUpFields, customfieldsDisplay);
	}

	/**
	 * 
	 * @param system
	 * @param processContext
	 * @return 
	 * 			if the there is not implementation returns Collections.EMPTY_LIST
	 * 			else returns list of fields to be removed
	 * @throws OIMMapperException
	 */
	public IOIMCacheableAdapter getAdapterObject(final Systems system,
			final Map<String, Object> processContext) throws OIMMapperException {
		AdapterLoader adapterLoader = new AdapterLoader();
		IOIMCacheableAdapter adapterObj = adapterLoader.getAdapterObj(session, system.getSystemId(), processContext,
				false);
		return adapterObj;
	}
	/**
	 * @param ptcCommentFields
	 * @param listContext
	 */
	private void changePTCCommentLookUp(final Map<String, String> ptcCommentFields,
			final List<Map<String, Object>> listContext) {

		// PTC Comment Fields LookUp

		Map<String, Object> changeKey = null;
		for (Map<String, Object> key : listContext) {
			if (key.get(UIConstants.KEY).equals(PTCConstants.PTC_COMMENT_FIELD_KEY)) {
				changeKey = key;
				break;
			}
		}
		if (changeKey != null)
			changeKey.put(DataType.LOOK_UP, ptcCommentFields);

	}

	/**
	 * @param list
	 * @param syncfieldname
	 * @return
	 */
	private List<Map<String, Object>> getListWithoutProperty(final List<Map<String, Object>> list,
			final String syncfieldname) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			map = (Map<String, Object>) iterator.next();
			if (map.containsValue(syncfieldname)) {
				iterator.remove();
				break;
			}
		}

		return list;
	}

	/**
	 * @param integrationId
	 * @param advanceConfigData
	 * @param entityNamList
	 * @param otherConfigInitialFieldVals
	 * @param formLoader
	 * @throws FormLoaderException
	 */
	private List<Map<String, Object>> getIntegrationContextForm(final Integer integrationId,
			final List<Entities> entityNamList, final HashMap<String, FieldValue> otherConfigInitialFieldVals,
			final FormLoader formLoader)
			throws FormLoaderException {
		List<Map<String, Object>> advanceConfigData = new ArrayList<Map<String, Object>>();
		Integer viewTypeId = getViewTypeId(session, ViewTypes.FORM);

		List<EntityViewTypeInfo> entityViewTypeInfoList = new ArrayList<>();

		for (int entityNameItr = 0; entityNameItr < entityNamList.size(); entityNameItr++) {
			Entities entityInfo = entityNamList.get(entityNameItr);
			entityViewTypeInfoList.add(new EntityViewTypeInfo(entityInfo.getEntityId(), viewTypeId));
		}
		List<Map<String, Object>> entityForm = formLoader.getCreateOrUpdateForm(entityViewTypeInfoList,
					integrationId, integrationId != -1, null, otherConfigInitialFieldVals);

			for (Map<String, Object> map : entityForm) {
				Object fieldName = map.get(UIConstants.KEY);
				if (fieldName != null && otherConfigInitialFieldVals != null
						&& otherConfigInitialFieldVals.get(fieldName) != null) {
					FieldValue additionalDetails = otherConfigInitialFieldVals.get(fieldName);
					if (additionalDetails != null) {
						Map<Integer, String> additionalLookups = additionalDetails.getLookup();
						if (additionalLookups != null) {
							Object existingLookups = map.get(UIConstants.LOOKUP);
							if (existingLookups instanceof Map) {
								((Map) existingLookups).putAll(additionalLookups);
							}
						}
						Object value = additionalDetails.getValue();
						if (value != null) {
							map.put(UIConstants.VALUE, value);
						}
					}
				}
				advanceConfigData.add(map);
			}

		return advanceConfigData;
	}

	/**
	 * @param sourceSystem
	 * @param installedProducts
	 * @param paramSystemID
	 * @return
	 */
	private List<Entities> getIntegrationContextEntities(final Systems sourceSystem) {
		List<Integer> installedProducts = OHProductInformationUtil.getProductIdsInstalled(session, companyId);
		String paramSystemID = SYSTEM_ID;
		String hqlGetAdvanceData = " from " + SystemEntityMapping.class.getName() + WHERE_SYSTEM_TYPE_SYSTEM_TYPE_ID
				+ paramSystemID + " " +
				AND_TYPE_TYPE_ID + Types.TYPE_OIM_ADVANCE + " and ohProduct.ohProductId in (:ohProductId)";
		Query qry = session.createQuery(hqlGetAdvanceData);
		qry.setInteger(paramSystemID, sourceSystem.getSystemType().getSystemTypeId());
		qry.setParameterList("ohProductId", installedProducts);
		List<SystemEntityMapping> entityNamList = qry.list();
		List<Entities> entityList = new ArrayList<>();
		for (SystemEntityMapping systemEntityMapping : entityNamList) {
			entityList.add(systemEntityMapping.getEntity());
		}
		Entities entity = session.get(Entities.class, Entities.POLLING_TYPE);
		entityList.add(entity);
		return entityList;
	}

	/**
	 * Method used for iterating the form values and making all the fields editable false and disabled false
	 * @param advanceDataMap
	 */

	private void makeAllFieldEditable(
			final HashMap<String, List<Map<String, Object>>> advanceDataMap) {
		Iterator allFormIterator = advanceDataMap.entrySet().iterator();
		List<Map<String, Object>> formRows = null;
		while (allFormIterator.hasNext()) {
			Map.Entry entry = (Entry) allFormIterator.next();
			formRows = (List<Map<String, Object>>) entry.getValue();
			for (Map<String, Object> map : formRows) {
					if(map.get(UIConstants.DISABLED)!=null && map.get(UIConstants.DISABLED).equals(Boolean.TRUE) && !map.get(UIConstants.KEY).equals(SourceSystem.PROCESSING_EVENT_INFO))
					map.put(UIConstants.DISABLED, false);
				if (map.get(UIConstants.EDITABLE) != null && map.get(UIConstants.EDITABLE).equals(Boolean.TRUE))
					map.put(UIConstants.EDITABLE, false);
			}
		}
	}

	@Override
	protected void modifyComboResult(final HashMap<String, Object> result,
			final String fieldName, final String entityName2) {
		ArrayList<String> keyList = (ArrayList<String>) result.get(UIConstants.KEY);
		List data = (List) result.get(UIConstants.DATA);
		if (entityName2.equals(AdminConstants.SYSTEM))
			modifyComboResultData(data, keyList, fieldName);
	}

	/**
	 * To modify custom combo data
	 * @param data : data list
	 * @param keyList : primary key list
	 * @param fieldName : display name of the field 
	 */
	private void modifyComboResultData(final List data, final ArrayList keyList, final String fieldName) {
		Map<Integer, String> systemsMap = null;
		if (fieldName.equals(UIConstants.SYSTEMS))
			systemsMap = EaiUtility.getOIMSystemsMap(session, UIConstants.SystemTypes.OIM_WITH_POLLER);
		else if (fieldName.equals(UIConstants.DESTINATION_SYSTEMS)) {
			systemsMap = EaiUtility.getOIMSystemsMap(session, UIConstants.SystemTypes.OIM_ONLY);
		}
		if (!fieldName.equals(UIConstants.OpsHubSystem.KEY_SMTP_SYSTEM)) {
			ArrayList<String> clonedKeyList = (ArrayList<String>) keyList.clone();
			Iterator<String> keyListItr = clonedKeyList.iterator();
			while (keyListItr.hasNext()) {
				Integer key = Integer.valueOf(keyListItr.next());
				if (!systemsMap.containsKey(key)) {
					int index = keyList.indexOf(String.valueOf(key));
					data.remove(index);
					keyList.remove(index);
				}

			}
		}
	}

	/**
	 * To get OIM configuration form
	 * @param session
	 * @param user
	 * @param entityName
	 * @param viewTypeName
	 * @param pKey
	 * @param edit
	 * @return
	 * @throws OpsHubBaseException
	 */
	public List<Map<String, Object>> getViewMetadata(final Session session,
			final UserInformation user, final String entityName, final String viewTypeName,
			final Integer pKey, final boolean edit,final boolean isClone) throws OpsHubBaseException{
		FormLoader formloader = new FormLoader(session, user.getCompanyId());
		Map<String, FieldValue> initialFieldVals = new HashMap<String, FieldValue>();
		Integer viewTypeId = getViewTypeId(session, viewTypeName);
		List<Map<String, Object>> form = formloader.getCreateOrUpdateForm(entityName, viewTypeId, pKey, edit, null,
				initialFieldVals);
		return updateBaseForm(form, isClone);
	}

	/**
	 * To update any data in the form
	 * @param form : form loader response
	 * @return : modified form data
	 */
	private List<Map<String, Object>> updateBaseForm(
			final List<Map<String, Object>> form,final boolean isClone) {
		Iterator<Map<String, Object>> formItr = form.iterator();
		while (formItr.hasNext()) {
			Map<java.lang.String, java.lang.Object> map = formItr
					.next();
			String key = (String) map.get(UIConstants.KEY);
			if (isClone && key.equals(UIConstants.INTEGRATION_NAME)) {
				String value = (String) map.get(UIConstants.VALUE);
				String integrationName = getNewIntegrationName(value);
				map.put(UIConstants.VALUE, integrationName);
			}
			/*
			 * If clone is the case we need to set the Source system list box editable for supporting change of source system
			 */
			if (isClone && key.equals(UIConstants.SYSTEMS)) {
				map.put(UIConstants.DISABLED, false);
			}
			if (key.equals(UIConstants.SYSTEMS) || key.equals(UIConstants.DESTINATION_SYSTEMS)) {
				Map systemsMap = null;
				if (key.equals(UIConstants.SYSTEMS))
					systemsMap = EaiUtility.getOIMSystemsMap(session, UIConstants.SystemTypes.OIM_WITH_POLLER);
				else if (key.equals(UIConstants.DESTINATION_SYSTEMS)) {
					systemsMap = EaiUtility.getOIMSystemsMap(session, UIConstants.SystemTypes.OIM_ONLY);
				}
				map.put(UIConstants.LOOKUP, systemsMap);
			}
		}

		return form;
	}

	/**
	 * To get the unique integration name for cloning
	 * @param value : initial integration name to be cloned
	 * @return unique name for cloned integration
	 */
	private String getNewIntegrationName(final String value) {
		boolean gotUniqueIntegrationName = false;
		int i = 0;
		String integrationName = value.concat(" - Copy");
		do {
			Query getUniqueIntegrationName = session.createQuery(" from " + EAIIntegrations.class.getName()
					+ " where baseIntegration.integrationName =:integrationName");
			getUniqueIntegrationName.setParameter("integrationName", integrationName);
			List<EAIIntegrations> integrationList = getUniqueIntegrationName.list();
			if (integrationList.size() > 0) {
				i++;
				integrationName = value.concat(" - Copy " + i);
			} else
				gotUniqueIntegrationName = true;

		} while (!gotUniqueIntegrationName);
		return integrationName;
	}

	/**
	 * To get the default workflow information
	 * @param session : hibernate session
	 * @return map containing name and id of workflow
	 */
	public Map<String, Object> getDefaultWorkflowForAllEvents(final Session session) {
		EaiProcessDefinition workflowDefaultInstance = session.get(EaiProcessDefinition.class, UIConstants.DEFAULTWORKFLOW_ID);
		HashMap hmReturn = new HashMap<String, Object>();
		if (workflowDefaultInstance == null)
			return hmReturn;
		hmReturn.put(UIConstants.WORKFLOW_NAME, workflowDefaultInstance.getProcessDefinitionName());
		hmReturn.put(UIConstants.WORKFLOW_ID, workflowDefaultInstance.getProcessDefinitionId());
		return hmReturn;
	}

	@Override
	protected void setInitialFieldValues(final Session session, final String entityName,
			final String viewTypeName, final Map<String, FieldValue> initialFieldVals,
			final Map<String, Object> setVals, final boolean edit, final Integer attributeId)
			throws OpsHubBaseException {
		String attrSourceSys = "sourceSystem";
		String hqlSystems = "select systemId, displayName from " + Systems.class.getName() + " where systemType.systemTypeId not in ("+SystemType.OPSHUB+","+SystemType.SMTP_MAIL_CLIENT_SYSTEM+") order by displayName ";
		String hqlJobStatus = "select id,statusName from JobStatus";
		Query qrySystems = session.createQuery(hqlSystems);
		Query qryJobStatus = session.createQuery(hqlJobStatus);
		FieldValue sysFieldVal = setFieldValue(qrySystems.list().iterator(), null, false);
		FieldValue jobStatusVals = setFieldValue(qryJobStatus.list().iterator(), null, false);
		initialFieldVals.put(attrSourceSys, sysFieldVal);
		initialFieldVals.put(STATUS, jobStatusVals);
	}

	/**
	 * To get Reconciliation configuration form
	 * @param session
	 * @param user
	 * @param entityName
	 * @param viewTypeName
	 * @param integrationId
	 * @param edit
	 * @return
	 * @throws OpsHubBaseException
	 */
	public List<Map<String, Object>> getReconciliationMetadata(final Session session,
			final UserInformation user, final String entityName, final String viewTypeName,
 final Integer reconciliationId, final boolean edit) throws OpsHubBaseException {

		FormLoader formloader = new FormLoader(session, user.getCompanyId());
		Map<String, FieldValue> initialFieldVals = new HashMap<String, FieldValue>();

		Criteria criteria = session.createCriteria(EaiProcessDefinition.class);
		criteria.add(Restrictions.eq("processDefinitionName", UIConstants.DEFAULTRECONCILIATIONWORKFLOW));
		EaiProcessDefinition epd = (EaiProcessDefinition) criteria.uniqueResult();
		FieldValue workflowField = null;
		if (epd != null) {
			workflowField = new FieldValue(epd.getProcessDefinitionId(), null, getWorkflowLookup());
		} else {
			workflowField = new FieldValue(null, null, getWorkflowLookup());
		}

		initialFieldVals.put(EAIReconciliations.WORKFLOW, workflowField);
		Integer viewTypeId = getViewTypeId(session, viewTypeName);
		List<Map<String, Object>> form = formloader.getCreateOrUpdateForm(entityName, viewTypeId, reconciliationId, edit,
				null, initialFieldVals);

		// if this is RESET and ACTIVE CALL then we will have already and integration ID other then -1 so get the reconcile name and make the
		// reconcile name as label
		// ELSE this will be call for new Configure reconcile
		if (!reconciliationId.equals(-1)) {

			if (edit) {
			EAIReconciliations eaiReconciliation = (EAIReconciliations) session.load(EAIReconciliations.class.getName(), reconciliationId);
				Map<String, Object> mapData = getMapFromListofMap(form, INTEGRATION);
				if (mapData != null & (!mapData.isEmpty())) {
					// Make the integration name LABEL
					mapData.put("type", "label");
					mapData.put("lookup", null);
					mapData.put("dataType", "1");
					mapData.put(VALUE, eaiReconciliation.getEaiIntegrations().getIntegrationName());
				}
			} else {
				Map<String, Object> mapData = getMapFromListofMap(form, INTEGRATION);
				if (mapData != null & (!mapData.isEmpty())) {
					mapData.put(VALUE, reconciliationId);
				}
			}
		}
		/*
		 * Replace mapping id with rule id
		 */
		replaceMappingIdWithRuleIds(form);
		return form;
	}

	/**
	 * This method saves/updates the reconciliation configuration data
	 * @param session
	 * @param user
	 * @param valueMap
	 * @param reconciliationId
	 * @param edit
	 * @return
	 * @throws OpsHubBaseException
	 * @throws SchedulerException
	 * @throws IOException
	 */
	public int saveReconciliationConfiguration(final Session session,
			final UserInformation user,final HashMap valueMap,
			final Integer reconciliationOrIntId, final boolean edit)
			throws OpsHubBaseException, SchedulerException, IOException {
		ReconciliationInfo reconcileInfo = new ReconciliationInfo();

		if (edit) {
			// when edit then reconciliationOrIntId holding the reconcile id in
			// case GWT.
			reconcileInfo.setReconciliationId(reconciliationOrIntId);
		} else {
			// when create then reconciliationOrIntId holding the integration id
			// in case of GWT.
			reconcileInfo.setIntegrationId(reconciliationOrIntId);
		}
		reconcileInfo.setReconciliationName((String) valueMap.get(UIConstants.RECONCILIATION_NAME));
		if ("0".equals(valueMap.get(UIConstants.SEARCH_IN_TARGET_SYSTEM))) {
			reconcileInfo.setTargetSearchQuery("");
			reconcileInfo.setTargetQueryCriteria(0);
		}
		else{
			reconcileInfo.setTargetSearchQuery((String) valueMap.get(UIConstants.TARGET_SEARCH_QUERY));
			reconcileInfo
					.setTargetQueryCriteria(Integer.parseInt((String) valueMap.get(UIConstants.TARGET_QUERY_CRITERIA)));
		}
		Integer workflowId = Integer.parseInt((String) valueMap.get(UIConstants.WORKFLOW));
		EaiProcessDefinition eaiProcessDefinition = (EaiProcessDefinition) session
				.get(EaiProcessDefinition.class.getName(), workflowId);
		EaiWorkflow eaiWorkflow = new EaiWorkflow(eaiProcessDefinition.getProcessDefinitionId(),
				eaiProcessDefinition.getProcessDefinitionName());
		reconcileInfo.setWorkflowDefination(eaiWorkflow);

		EAIReconciliations eaiReconcileObj = createOrUpdateReconciliation(session, user, reconcileInfo, edit, true);
		return eaiReconcileObj.getReconcileJobInstance().getJobInstanceId();

	}

	/**
	 * @param session
	 * @param userInformation
	 * @param request
	 * @return
	 * @throws IOException
	 * @throws SchedulerException
	 * @throws OpsHubBaseException
	 */
	public ReconcileValidatorResponse createOrUpdateReconciliation(final Session session,
			final UserInformation userInformation, final List<ReconciliationInfo> recoList)
			throws OpsHubBaseException, SchedulerException, IOException {

		List<ResponseValidatiorObject> validationListForCreateUpdate = new ArrayList<>();
		List<Integer> integrationIds = new ArrayList<>();
		for (ReconciliationInfo recoInfo : recoList) {
			ResponseValidatiorObject ruleValidation = validateConfigurationForReconcile(recoInfo.getIntegrationId(),
					session);
			if (ruleValidation != null) {
				validationListForCreateUpdate.add(ruleValidation);
				continue;
			}
			// graph ql API invoked this method we decide the reconcile edit or
			// create based reco object existence from the given integration id.
			// its is expected to have integration id. create or edit performed
			// using integration id
			// compatibility passing boolean for graph ql api
			EAIReconciliations foundReco = getReconciliationInfoFromId(session, recoInfo.getIntegrationId());
			// create of update using integration id only no need to use
			// the reco id

			boolean isEdit = foundReco != null;
			if (isEdit) {
				// in case of update expected only integration id. so we are
				// setting reco id as expected by method need to invoke for
				// create update
				recoInfo.setReconciliationId(foundReco.getReconciliationId());
			}
				/*
			 * whether create or update. reset the reocncile setting from
			 * integartions fetch the target loop setting and reconcile name
			 * parameter from integration. i.e. now reco target look up =
			 * integration target look up similarly reco name = integration
			 * name. in case of create reco we need to fetch from integration
			 * and set in reco object in case of update, when integration gets
			 * updated reco name and reco target look up setting should updated
			 * directly from integration update request only
				 */
				// modify to update with target look up setting and reconcile
				// name
				modifyReconcileInfo(recoInfo, session);


			// reco status will change with separate request from ui
			createOrUpdateReconciliation(session, user, recoInfo, isEdit, false);
			integrationIds.add(recoInfo.getIntegrationId());

		}
		ReconcileValidatorResponse reconcileValidatorObj = new ReconcileValidatorResponse();
		reconcileValidatorObj.setValidationsResultList(validationListForCreateUpdate);
		reconcileValidatorObj.setIdsList(integrationIds);

		return reconcileValidatorObj;
	}

	/**
	 * 
	 * 
	 * @param recoInfo
	 * @throws EAIActionHandlerException
	 *             modify to update with target look up setting and reconcile
	 *             name
	 */
	private void modifyReconcileInfo(final ReconciliationInfo recoInfo, final Session session)
			throws EAIActionHandlerException {
		EAIIntegrations eaiIntegrations = session.get(EAIIntegrations.class, recoInfo.getIntegrationId());
		recoInfo.setReconciliationName(eaiIntegrations.getIntegrationName());
		TargetLookup targetLookUpContext = getTargetLookUpForIntegration(session, eaiIntegrations);
		if (targetLookUpContext.getIsTargetLookup() != null && targetLookUpContext.getIsTargetLookup().equals("1")) {
			recoInfo.setTargetSearchQuery(targetLookUpContext.getTargetLookUpQuery());
			// if target look up expected that integration has value for
			// criteria
			recoInfo.setTargetQueryCriteria(Integer.parseInt(targetLookUpContext.getIsContinueUpdate()));
		} else {
			recoInfo.setTargetSearchQuery("");
			recoInfo.setTargetQueryCriteria(0);
		}

	}

	/**
	 * @param session
	 * @param integrationId
	 * @return
	 * @throws EAIActionHandlerException
	 */
	private TargetLookup getTargetLookUpForIntegration(final Session session, final EAIIntegrations eaiIntegrations)
			throws EAIActionHandlerException {
		HashMap<String, Object> destProcessContext = new HashMap<String, Object>();

		if (eaiIntegrations.getSetProcessDefinition() != null) {
			Iterator<EaiEventProcessDefinition> processContextSet = eaiIntegrations.getSetProcessDefinition()
					.iterator();
			if (processContextSet.hasNext()) {
				EaiEventProcessDefinition eaiEventProcessDef = processContextSet.next();
				int eventProcessDefinitionId = eaiEventProcessDef.getEventProcessDefinitionId();
				destProcessContext.putAll(EaiUtility.getProcessContext(session, eaiEventProcessDef,
						eaiIntegrations.getTargetSystem().getSystemId(), false));
			}
		}

		return TargetLookup.fetchTargetLookupContext(destProcessContext);
	}


	public EAIReconciliations createOrUpdateReconciliation(final Session session, final UserInformation user,
			final ReconciliationInfo reconcileInfo, final boolean edit, final boolean isUniqueNameToCheck)
			throws OpsHubBaseException, IOException {

		LicenseVerifier licVerifier = new LicenseVerifier(session);
		licVerifier.verify();

		JobInstance jobInstance = null;
		String oldReconciliationName = "";
		EAIIntegrations eaiIntegrations;
		EAIReconciliations eaiReconciliatons = null;
		if (edit) {
			eaiReconciliatons = (EAIReconciliations) session.load(EAIReconciliations.class.getName(),
					reconcileInfo.getReconciliationId());
			eaiReconciliatons.validateAndSetEntityVersion(reconcileInfo.getObjectVersion());
			jobInstance = eaiReconciliatons.getReconcileJobInstance();
			eaiIntegrations = eaiReconciliatons.getEaiIntegrations();

			LoggerManager objLoggerManager = new LoggerManager();
			oldReconciliationName = objLoggerManager
					.getReconciliationNameFromId(eaiReconciliatons.getReconciliationId());
		} else {
			jobInstance = new JobInstance();
			eaiIntegrations = (EAIIntegrations) session.get(EAIIntegrations.class.getName(),
					reconcileInfo.getIntegrationId());
			jobInstance.setStatus((JobStatus) session.get(JobStatus.class.getName(), JobStatus.PAUSED));
			jobInstance.setJobGroup((JobGroup) session.get(JobGroup.class.getName(), JobGroup.RECONCILATION_JOB));

			String licenseEdition = EditionDetailsLoader.getInstance().getInstanceEdition(session);
			jobInstance.setSchedule((JobSchedule) session.get(JobSchedule.class.getName(),
					FreeEditionGlobalScheduleLimitInitializer.getDefaultScheduleForLicense(licenseEdition)));
			jobInstance.setJobName(reconcileInfo.getReconciliationName() + "_job");
			jobInstance.setOimIntegration(eaiIntegrations);

		}
		jobInstance.setJob((Job) session.get(Job.class.getName(), Job.RECONCILATION_JOB));
		session.saveOrUpdate(jobInstance);
		return postProcessAfterCreateUpdateJobInstance(session, reconcileInfo, edit, isUniqueNameToCheck, jobInstance,
				oldReconciliationName, eaiIntegrations, eaiReconciliatons);

	}

	/**
	 * @param session
	 * @param reconcileInfo
	 * @param edit
	 * @param isUniqueNameToCheck
	 * @param jobInstance
	 * @param oldReconciliationName
	 * @param eaiIntegrations
	 * @param eaiReconciliatons
	 * @return
	 * @throws LicenseException
	 * @throws DataValidationException
	 * @throws IOException
	 * @throws ORMException
	 * @throws LoggerManagerException
	 * @throws OpsHubBaseException
	 */
	private EAIReconciliations postProcessAfterCreateUpdateJobInstance(final Session session,
			final ReconciliationInfo reconcileInfo, final boolean edit, final boolean isUniqueNameToCheck,
			final JobInstance jobInstance, final String oldReconciliationName, final EAIIntegrations eaiIntegrations,
			EAIReconciliations eaiReconciliatons) throws LicenseException, DataValidationException, IOException,
			ORMException, LoggerManagerException, OpsHubBaseException {

		EaiProcessDefinition eaiProcessDefinition = session.get(EaiProcessDefinition.class,
				reconcileInfo.getWorkflowDefination().getProcessDefnId());

		eaiReconciliatons = constructReconcileObj(eaiIntegrations, jobInstance, reconcileInfo,
				eaiProcessDefinition, eaiReconciliatons);
		validateUniqueReconcile(session, eaiReconciliatons, edit, isUniqueNameToCheck);
		session.saveOrUpdate(eaiReconciliatons);

		if (!edit || JobStatus.EXPIRED.equals(eaiReconciliatons.getReconcileJobInstance().getStatus().getId())) {
			setEventTimeMarkers(edit, eaiIntegrations, eaiReconciliatons);
			// On reset delete the job context
			for (JobContext jc : jobInstance.getJobContext()) {
				session.delete(jc);
			}
			//Flushing session after deleting the context. So that, duplicate add operation on criteria_processed_till_date doesn't happen
			session.flush();
			//Setting job context in job instance to empty because reco is expired and will be treated as new.
			jobInstance.setJobContext(new HashSet<>(0));
			jobInstance.setStatus(session.get(JobStatus.class, JobStatus.PAUSED));
			session.save(jobInstance);
			// create log file only if it is a create event
			if (!edit) {
				jobInstance.setEaiReconcile(eaiReconciliatons);
			}
		}
		//Handle migration license validity when criteria is configured in integration
		handleMigrationLicenseValidity(session, eaiIntegrations, jobInstance,
				reconcileInfo.getSuppressValidationWarning() == null ? false
						: reconcileInfo.getSuppressValidationWarning());

		// check for rename of reconciliation name
		if (!oldReconciliationName.isEmpty()
				&& !oldReconciliationName.equals(eaiReconciliatons.getReconciliationName())) {
			// change reconciliation file name and appender
			changeLogFileName(eaiReconciliatons.getReconciliationId(), oldReconciliationName,
					LoggerManager.RECONCILIATION);
		}

		// update integration mode. create or update set mode to reconcile
		// otherwise on reset it wont set
		// in case reconcile configuration set the corresponding integration
		// mode to reconcile.
		setIntegrationMode(session, eaiIntegrations, ConfigMode.RECONCILE_MODE_ID);

		return eaiReconciliatons;
	}

	public String switchToIntegrateMode(final Session session, final List<Integer> ids, final Integer mode)
			throws OpsHubBaseException {

		if (mode == null || !mode.equals(ConfigMode.INTEGRATION_MODE_ID)) {
			throw new OpsHubBaseException("012050", new String[] { SWITCHING_TO_INTEGRATION_MODE,
					"Expected Mode input is:" + ConfigMode.INTEGRATION_MODE_ID + " Actual Mode Input is:" + mode },
					null);
		}

		for (Integer integrationId : ids) {
			EAIIntegrations eaiIntegrations = session.get(EAIIntegrations.class, integrationId);
			EAIReconciliations eaireconciliations = getReconciliationInfoFromId(session, integrationId);
			if (eaireconciliations != null
					&& !JobStatus.EXPIRED.equals(eaireconciliations.getReconcileJobInstance().getStatus().getId())) {
				throw new OpsHubBaseException("012050",
						new String[] { SWITCHING_TO_INTEGRATION_MODE,
								"Can not switch to integration mode, as reconciliation execution is not completed for the integration:"
										+ integrationId },
						null);
			}
			setIntegrationMode(session, eaiIntegrations, mode);
		}
		return "Mode Switch to Integration for Integration(s):" + ids;

	}

	/**
	 * @param session
	 * @param eaiIntegrations
	 * @throws OpsHubBaseException
	 */
	private void setIntegrationMode(final Session session, final EAIIntegrations eaiIntegrations, final Integer mode)
			throws OpsHubBaseException {

		if (eaiIntegrations != null) {
			OpsHubLoggingUtil.debug(LOGGER,
					"Setting config mode as:" + mode + " for integration:" + eaiIntegrations.getIntegrationId(), null);

			eaiIntegrations.setConfigMode(mode);
			session.saveOrUpdate(eaiIntegrations);

		}
		 else {
			throw new OpsInternalError("012064",
					new String[] { UIConstants.CONFIG_MODE},
					null);
		}


	}

	/**
	 * @param jobInstance
	 * @param eaiIntegrations
	 * @param reconcileInfo
	 * @param eaiReconciliatons
	 * @param eaiProcessDefinition2
	 * @return
	 * @throws DataValidationException
	 */
	private EAIReconciliations constructReconcileObj(final EAIIntegrations eaiIntegration,
			final JobInstance jobInstance, final ReconciliationInfo reconcileInfo,
			final EaiProcessDefinition eaiProcessDefinition, final EAIReconciliations eaiReconciliatons)
			throws DataValidationException {
		EAIReconciliations eaiReconciliaton = eaiReconciliatons == null ? new EAIReconciliations() : eaiReconciliatons;

		eaiReconciliaton.setReconciliationId(reconcileInfo.getReconciliationId());
		eaiReconciliaton.setReconciliationName(reconcileInfo.getReconciliationName());
		eaiReconciliaton.setTargetSearchQuery(reconcileInfo.getTargetSearchQuery());
		eaiReconciliaton.setTargetQueryCriteria(reconcileInfo.getTargetQueryCriteria());
		eaiReconciliaton.setEaiProcessDefiniton(eaiProcessDefinition);
		eaiReconciliaton.setEaiIntegrations(eaiIntegration);
		eaiReconciliaton.setReconcileJobInstance(jobInstance);

		return eaiReconciliaton;

	}

	public ResponseValidatiorObject validateConfigurationForReconcile(final Integer integrationId,
			final Session session) throws EAIActionHandlerException {

		List<String> errorMessage = new ArrayList<>();

		// 1. validate integration status
		EAIIntegrations eaiIntegrations = (EAIIntegrations) session.get(EAIIntegrations.class.getName(), integrationId);
		if (EAIIntegrationStatus.ACTIVE.equals(eaiIntegrations.getStatus().getId())) {
			errorMessage.add(ReconcileValidation.ERROR_MSG_FOR_RECONCILE_INTEGRATION_ACTIVE);

		}
		// 2. validate rule settings

		int ruleId = getReconcileRuleId(session, integrationId);
		if (ruleId == 0) {
			errorMessage.add(ReconcileValidation.ERROR_MSG_RECONCILE_RULE_NOT_CONFIGURED);
		}
		if (errorMessage != null && !errorMessage.isEmpty())
			return new ResponseValidatiorObject(String.valueOf(integrationId), "Reconcile", errorMessage);

		return null;
	}

	public EAIReconciliations getReconciliationInfoFromIntegration(final Session session, final Integer integrationId)
			throws OpsHubBaseException {
		EAIReconciliations eaiReco = getReconciliationInfoFromId(session, integrationId);

		if (eaiReco == null) {
			throw new OpsHubBaseException("015508", new Object[] { integrationId }, null);
		}
		return eaiReco;
	}

	private void replaceMappingIdWithRuleIds(final List<Map<String, Object>> form) {
		for (Map<String, Object> map : form) {
			String key = (String) map.get(KEY);
			if (key != null && key.equals(EAIReconciliations.RECONCILE_RULES)) {
				Map<Integer, String> lookupValues = (Map<Integer, String>) map.get("lookup");
				Map<Integer, String> lookupRulesValues = new HashMap<Integer, String>();
				if (lookupValues != null && lookupValues.size() > 0) {
					for (Integer mappingId : lookupValues.keySet()) {
						String mappingName = lookupValues.get(mappingId);
						Scripts script = session.load(Scripts.class, mappingId);
						Integer ruleScriptId = script.getRuleScript().getRuleScript().getScriptId();
						lookupRulesValues.put(ruleScriptId, mappingName);
					}
					lookupValues.clear();
					lookupValues.putAll(lookupRulesValues);
				}
				break;
			}
		}
	}

	private void setEventTimeMarkers(final boolean edit, final EAIIntegrations eaiIntegrations, final EAIReconciliations eaiReconciliatons){

		String jobInstanceCondition = "";
		if (!edit) {
			jobInstanceCondition = "integration.sourceJobInstance.jobInstanceId";
		} else {
			jobInstanceCondition = "reconciliation.reconcileJobInstance.jobInstanceId";
		}

		Query eventsQuery = session
				.createQuery(FROM + EventTime.class.getName() + " where " + jobInstanceCondition + "=:jobInstanceId");
		if (!edit) {
			// Insert eventTime for reconciliation events
			eventsQuery.setParameter(PARAM_JOB_INSTANCE, eaiIntegrations.getSourceJobInstance().getJobInstanceId());
		}
		else if(edit && JobStatus.EXPIRED.equals(eaiReconciliatons.getReconcileJobInstance().getStatus().getId())){
			// update eventTime for reconciliation events
			eventsQuery.setParameter(PARAM_JOB_INSTANCE,eaiReconciliatons.getReconcileJobInstance().getJobInstanceId());
		}

		Iterator<EventTime> eventsItr = eventsQuery.list().iterator();
		while (eventsItr.hasNext()) {
			EventTime eventTime = eventsItr.next();
			EventTime updateEventTime = null;
			if (!edit) {
				updateEventTime = new EventTime();
			}
			else if(edit && JobStatus.EXPIRED.equals(eaiReconciliatons.getReconcileJobInstance().getStatus().getId())){
				updateEventTime = eventTime;
			}

			updateEventTime.setReconciliation(eaiReconciliatons);
			updateEventTime.setEventStatus(1);
			updateEventTime.setLastProcessedEventTime(Long.parseLong("-1"));
			updateEventTime.setProcessingEventId("-1");
			updateEventTime.setProcessingRevisionId("0");
			updateEventTime.setProcessingEventTime(Long.parseLong("-1"));
			updateEventTime.setLastProcessedEventId("-1");
			updateEventTime.setLastUpdatedOn(new Timestamp(System.currentTimeMillis()));
			updateEventTime.setProcessedRevisionId("0");
			updateEventTime.setProjectKey(eventTime.getProjectKey());
			session.saveOrUpdate(updateEventTime);
		}
	}

	public List<String> changeReconcileStatus(final Session session, final List<String> reconciliationIds,
			final String status, final boolean suppressValidationWarning, final boolean isGraphQL)
			throws OpsHubBaseException, SchedulerException {
		StatusValidation statusValidation = changeAndValidateReconcileStatus(session, reconciliationIds, status,
				suppressValidationWarning);
		List<String> responseList = new ArrayList<>();

		if (isGraphQL) {
			GroupMessageInfo.addGroupMessageString(responseList, statusValidation.getFailureMsgInfo(),
					ReconcileValidation.RECONCILE, GroupMessageInfo.WARNING);
			GroupMessageInfo.addGroupMessageString(responseList, statusValidation.getLicenseMsgInfo(),
					ReconcileValidation.RECONCILE, GroupMessageInfo.ERROR);

		} else {
			GroupMessageInfo.addMessageStringWithoutGroup(responseList, statusValidation.getSuccessMsgInfo());
			GroupMessageInfo.addMessageStringWithoutGroup(responseList, statusValidation.getFailureMsgInfo());
			GroupMessageInfo.addMessageStringWithoutGroup(responseList, statusValidation.getLicenseMsgInfo());
		}
		return responseList;
	}

	public List<String> changeReconcileStatus(final Session session, final List<String> reconciliationIds,
			final String status) throws OpsHubBaseException, SchedulerException {
		//Setting suppress validation warning to true, because we don't want reco to display any error when migration license is not present.
		return changeReconcileStatus(session, reconciliationIds, status, true, false);
	}

	private StatusValidation changeAndValidateReconcileStatus(final Session session,
			final List<String> reconciliationIds, final String status, final boolean suppressValidationWarning)
			throws OpsHubBaseException, SchedulerException {
		String toStatus;
		List<String> reconcileNameList = null;
		if (JobStatus.INACTIVE.equals(status))
			toStatus = "PAUSED";
		else if (JobStatus.RUN_STATUS.equals(status))
			toStatus = "ACTIVE";
		else
			toStatus = "EXPIRED";

		List<GroupMessageInfo> sucessMessageInfoLst = new ArrayList<GroupMessageInfo>();
		List<GroupMessageInfo> failureMessgeInfoLst = new ArrayList<GroupMessageInfo>();
		List<GroupMessageInfo> licenseMessageInfoLst = new ArrayList<GroupMessageInfo>();

		// For each requested reconcile to get activated, Check update event mapping have rule enabled in referenced integration.
		// If any of reconcile's integration does not have rule enabled mapping in update event,then do not activate any reconcile and give proper
		// message with reconcile name
		// Here status will come Run while request of ACTIVATE reconcile
		if (RUN.equals(status)) {
			reconcileNameList = checkReconciliationsForRule(reconciliationIds);
		}
		// Now check that no reconcile is using this mapping associated rule so it can delete the rule associated
		if (reconcileNameList == null || reconcileNameList.isEmpty()) {
			for (String reconciliationId : reconciliationIds) {
				EAIReconciliations eaiReconciliations = (EAIReconciliations) session.load(EAIReconciliations.class.getName(),
						Integer.parseInt(reconciliationId));
				String groupName = eaiReconciliations.getEaiIntegrations().getBaseIntegration().getIntegrationGroup()
						.getGroupName();
				Integer groupId = eaiReconciliations.getEaiIntegrations().getBaseIntegration().getIntegrationGroup()
						.getGroupId();
				Integer entityPairId = eaiReconciliations.getEaiIntegrations().getBaseIntegration()
						.getIntegrationBaseId();
				GroupIntegrationInfo groupIntegrationInfo = new GroupIntegrationInfo(groupName,
						eaiReconciliations.getReconciliationName(), groupId, eaiReconciliations.getReconciliationId(),
						entityPairId);

				if (RUN.equals(status)) {

					//check whether other direction reconcile or integration running then not allowed reconcile to activate
					String notAllowedAsRecoRunning = isReconcileConfiguredOtherDirection(session,
							eaiReconciliations.getEaiIntegrations(), status, ConfigMode.RECONCILE);
					if (!EaiUtility.isNullOrEmpty(notAllowedAsRecoRunning)) {
						failureMessgeInfoLst.add(new GroupMessageInfo(groupIntegrationInfo, notAllowedAsRecoRunning));
						continue;
					}
					String editionVerficationMessageForCurrentIntegration = validateOIMEditionFeatures(eaiReconciliations.getEaiIntegrations(), null);
					if (editionVerficationMessageForCurrentIntegration != null) {
						licenseMessageInfoLst
								.add(new GroupMessageInfo(groupIntegrationInfo,
										new StringBuffer(ReconcileValidation.ERROR_MSG_FOR_RECONCILE_LICENSE)
												.append(" ").append(editionVerficationMessageForCurrentIntegration)
												.toString()));
						continue;
					}

				}
				String currentStatus = eaiReconciliations.getReconcileJobInstance().getStatus().getStatusName();
				boolean execStatus = changReconcileJobStatus(session, eaiReconciliations.getReconcileJobInstance(),
						status, suppressValidationWarning);
				if (!execStatus) {
					failureMessgeInfoLst.add(new GroupMessageInfo(groupIntegrationInfo,
							new StringBuffer(IntegrationValidation.INVALID_TRANSIOTION).append(" from ")
									.append(currentStatus).append(" to ").append(toStatus).toString()));
				} else {

					sucessMessageInfoLst.add(new GroupMessageInfo(groupIntegrationInfo,
							new StringBuffer(ReconcileValidation.SUCCESSFULL_TRANSTION).append(" ").append(toStatus)
									.toString()));
				}
			}
		} else {
			String reconcileNames = StringUtils.join(reconcileNameList.toArray(), ',');
			throw new OpsHubBaseException("015506", new String[] { reconcileNames }, null);
		}
		StatusValidation statusValidation = new StatusValidation();
		statusValidation.setSuccessMsgInfo(sucessMessageInfoLst);
		statusValidation.setFailureMsgInfo(failureMessgeInfoLst);
		statusValidation.setLicenseMsgInfo(licenseMessageInfoLst);

		return statusValidation;
	}

	/*
	 * This is done with reference to requirement 100936.
	 * When criteria is configured in integration, we are checking whether migration license is present or not.
	 * 
	 * If yes, then reconciliation start time is valid and can be started from EPOCH in case of create.
	 * If no, then reconciliation start time is not valid and have to be set to [old license start date - 60 days]
	 */
	private void handleMigrationLicenseValidity(final Session session, final EAIIntegrations eaiIntegrations,
			final JobInstance jobInstance,
			final boolean suppressValidationWarning) throws LicenseException {
		ReconcilationWithMigrationLicenseVerifier recoMigLicVerifier = new ReconcilationWithMigrationLicenseVerifier(
				session);
		boolean isStartDateProcessingRequired = recoMigLicVerifier.isReconcilationDateRequireChange(eaiIntegrations, jobInstance);
		
		if(isStartDateProcessingRequired){
			Date startProcessingDate = recoMigLicVerifier.getValidStartDateForReconcilation(jobInstance);
			if (!suppressValidationWarning) {
				throw new MigrationLicenseException("015029", new String[] {}, null,
						String.valueOf(startProcessingDate.getTime()));
			}
			setCriteriaProcessedDateInJobContext(session, startProcessingDate, jobInstance);		
		}
	}

	private List<String> checkReconciliationsForRule(final List<String> reconciliationIds) {
		List<String> reconcileNameList = new ArrayList<String>();
		for (String reconcileId : reconciliationIds) {
			EAIReconciliations eaiReconciliations = (EAIReconciliations) session.load(EAIReconciliations.class.getName(),
					Integer.parseInt(reconcileId));
			int id = eaiReconciliations.getReconciliationId();
			int integrationId = getIntegrationIdFromReconcile(session, id);
			int ruleId = getReconcileRuleId(session, integrationId);
			if (ruleId == 0) {
				reconcileNameList.add(eaiReconciliations.getReconciliationName());
			}
		}
		return reconcileNameList;
	}

	public boolean changReconcileJobStatus(final Session session, final JobInstance job, final String status,
			final boolean suppressValidationWarning)
			throws ScheduleCreationException, OpsHubBaseException, SchedulerException {
		Integer currentStatus = job.getStatus().getId();
		JobSchedular schedular = new JobSchedular();
		if(JobStatus.PAUSED.equals(currentStatus) && !(JobStatus.RUN_STATUS.equals(status) || JobStatus.EXPIRED_STATUS.equals(status))){
			return false;
		}
		if(JobStatus.ACTIVE.equals(currentStatus) && !(JobStatus.EXPIRED_STATUS.equals(status) || JobStatus.INACTIVE.equals(status))){
			return false;
		}
		if (JobStatus.EXPIRED.equals(currentStatus)) {
			return false;
		}
		if (status.equals(JobStatus.RUN_STATUS)) {
			// Check license status before activating reconcile configuration
			LicenseVerifier licVerifier = new LicenseVerifier(session);
			licVerifier.verify();
			// Get EAIReconciliations Object from job Object
			EAIReconciliations eaiReconciliations = job.getEaiReconcile();

			// Get EAIIntegrations Object from job EAIReconciliations
			EAIIntegrations eaiIntegrations = eaiReconciliations.getEaiIntegrations();

			// Validate Edition Features
			StringBuffer errorMessage = new StringBuffer("");
			String editionVerficationMessageForCurrentIntegration = validateOIMEditionFeatures(eaiIntegrations, null);
			if (editionVerficationMessageForCurrentIntegration != null) {
				errorMessage.append(eaiReconciliations.getReconciliationName() + " reconcile can not be activated").append(
						editionVerficationMessageForCurrentIntegration);
				throw new LicenseException("015009", new String[] { errorMessage.toString() }, null);
			}
			handleMigrationLicenseValidity(session, eaiReconciliations.getEaiIntegrations(),
					eaiReconciliations.getReconcileJobInstance(), suppressValidationWarning);
			// Change the Status of the Associated Integration to Inactive if ACTIVE
			if (eaiIntegrations.getStatus().getId().equals(EAIIntegrationStatus.ACTIVE)) {
				List<String> integrationList = new ArrayList<String>();
				// create a list holding the integration ID to be get INACTIVATATED.
				integrationList.add(eaiIntegrations.getIntegrationId().toString());

				// Change the integration status
				changeStatus(session, integrationList, IN_ACTIVE);
			}

			// Delete all the failed events of the Associated Integration
			deleteAssociatedJobFailedEvent(eaiIntegrations.getSourceJobInstance().getJobInstanceId());

			schedular.doRequiredAction(job.getJobInstanceId(), BGPJobConstants.RUN);
		} else if (status.equals(JobStatus.INACTIVE)) {
			schedular.doRequiredAction(job.getJobInstanceId(), BGPJobConstants.PAUSE);
		} else if (status.equals(JobStatus.EXPIRED_STATUS)) {
			deleteAssociatedJobFailedEvent(job.getJobInstanceId());
			schedular.doRequiredAction(job.getJobInstanceId(), BGPJobConstants.DELETE);
		}
		return true;
	}

	/*
	 * This method returns status of a reconciliation
	 */
	public static int getReconciliationEditStatus(final Session session, final int integrationId) {
		Query query = null;
		query = session.createQuery(FROM + EAIReconciliations.class.getName() +  " where eaiIntegrations.integrationId=:integrationId");
		query.setParameter(INTEGRATION_ID, integrationId);
		if (query.list().size() == 1) {
			EAIReconciliations eaiReconciliations = (EAIReconciliations) query.uniqueResult();
			return eaiReconciliations.getReconciliationId();

		}
		else 
			return -1;
	}

	/*
	 * This method returns configuration data of a reconciliation given the integration Id
	 */
	public List<Map<String, Object>> getReconciliationConfigurationData(final Session session,
			final UserInformation user, final String entityName, final String viewTypeName,
 final Integer reconciliationId, final boolean isClone, final boolean edit) throws OpsHubBaseException,
			SchedulerException {

	
		EAIReconciliations eaiReconciliation= (EAIReconciliations)session.load(EAIReconciliations.class.getName(), reconciliationId);
		Integer integrationId = eaiReconciliation.getEaiIntegrations().getIntegrationId();
		if (!isClone) {
			if (JobStatus.EXPIRED.equals(eaiReconciliation.getReconcileJobInstance().getStatus().getId())) {
				return getReconciliationMetadata(session, user, entityName, viewTypeName, reconciliationId, true);
			}
		}
		List<String> reconciliationIds = Arrays.asList(String.valueOf(eaiReconciliation.getReconciliationId()));
		//in case of Reconcile in Active State : And Clone: No need to change state to pause
		//in case of Reconcile in Pause State: And Clone: in this case currently following code is returning the false hence and no need to call 
		//this method call in case of Edit: Available when state is paused and below code result in no state changes internally false will return so no need to handle
		//this method call in case of reset and activate  in which isClone Is false which handle using above code in which state is expired hence below code not executed
		// so adding check of !isClone as no need to invoke in case of clone other cases handle internally
		if (edit) {
			changeReconcileStatus(session, reconciliationIds, JobStatus.INACTIVE);
		}
		Map<String, FieldValue> initialFieldVals = new HashMap<String, FieldValue>();
		FieldValue workflowField = new FieldValue(eaiReconciliation.getEaiProcessDefiniton().getProcessDefinitionId(), null,getWorkflowLookup());
		initialFieldVals.put(EAIReconciliations.WORKFLOW, workflowField);
		EAIIntegrations eaiIntegration = getIntegrationInfoFromId(integrationId);
		FieldValue integrationName = new FieldValue(integrationId, eaiIntegration.getIntegrationName());
		initialFieldVals.put(EAIReconciliations.INTEGRATION, integrationName);

		if (eaiReconciliation.getTargetSearchQuery() == null || eaiReconciliation.getTargetSearchQuery().isEmpty())
			initialFieldVals.put(EAIReconciliations.SEARCH_IN_TARGET_SYSTEM, new FieldValue(0));
		else
			initialFieldVals.put(EAIReconciliations.SEARCH_IN_TARGET_SYSTEM, new FieldValue(1));

		initialFieldVals.put(EAIReconciliations.TARGET_SEARCH_QUERY, new FieldValue(eaiReconciliation.getTargetSearchQuery()));
		initialFieldVals.put(EAIReconciliations.TARGET_QUERY_CRITERIA, new FieldValue(eaiReconciliation.getTargetQueryCriteria()));

		FormLoader formloader = new FormLoader(session, user.getCompanyId());
		Integer viewTypeId = getViewTypeId(session, viewTypeName);
		List<Map<String, Object>> form = formloader.getCreateOrUpdateForm(entityName, viewTypeId, reconciliationId, true,
				null, initialFieldVals);
		replaceMappingIdWithRuleIds(form);
		if (isClone) {
			//If isClone TRUE then change the name Default to "ReconcileName - Copy"
			Map<String, Object> reconcileNameFormData = getMapFromListofMap(form, "reconciliationName");
			if (reconcileNameFormData != null && !reconcileNameFormData.isEmpty()) {
				String reconcileName = (String) reconcileNameFormData.get(VALUE);
				StringBuffer strBuffer = new StringBuffer(reconcileName);
				strBuffer.append("- Copy");
				reconcileName = strBuffer.toString();
				reconcileNameFormData.put(VALUE, reconcileName);
			}
		} else {
			// In case of Edit reconcile ,mark the integration Name as Label as in EDIT operation we can not change the selection of Integration
			Map<String, Object> formData = getMapFromListofMap(form, INTEGRATION);
			formData.put("type", "label");
			formData.put("lookup", null);
			formData.put("dataType", "1");
		}
		return form;
	}

	public EAIIntegrations getIntegrationInfoFromId(final Integer integrationId) {
		return (EAIIntegrations) session.get(EAIIntegrations.class.getName(), integrationId);
	}

	private Map<Integer, String> getWorkflowLookup() {
		Map<String, String> workflows = Action.getAllWorkflows(session, 2);
		Map<Integer, String> mapOfWorkflows = new HashMap<Integer, String>();

		for (String key : workflows.keySet()) {
			mapOfWorkflows.put(Integer.parseInt(key), workflows.get(key));
		}
		return mapOfWorkflows;
	}

	public boolean getReconciliationJobStatus(final Session session, final Integer integrationId) {
		Query query = session.createQuery(FROM + EAIReconciliations.class.getName() +  " where eaiIntegrations.integrationId=:integrationId");
		query.setParameter(INTEGRATION_ID, integrationId);
		if (query.list().size() > 0) {
			EAIReconciliations eaireconciliations = (EAIReconciliations) query.list().get(0);
			if(eaireconciliations != null && JobStatus.EXPIRED.equals(eaireconciliations.getReconcileJobInstance().getStatus().getId()) || JobStatus.PAUSED.equals(eaireconciliations.getReconcileJobInstance().getStatus().getId())){
				return true;
			}
		}
		return false;
	}

	public static EAIReconciliations getReconciliationInfoFromId(final Session session, final Integer integrationId) {
		Query query = session.createQuery(FROM + EAIReconciliations.class.getName() +  " where eaiIntegrations.integrationId=:integrationId");
		query.setParameter(INTEGRATION_ID, integrationId);
		if (query.list().size() > 0)
			return (EAIReconciliations) query.list().get(0);
		else
			return null;
	}

	private void renameLogFile(final int id, final String logType) throws ORMException, LoggerManagerException {
		final String integrationOrReconcilName;
		LoggerManager objLoggerManager = new LoggerManager();
		// check if it is integration or reconciliation
		if (LoggerManager.RECONCILIATION.equals(logType)) {
			integrationOrReconcilName = objLoggerManager.getReconciliationNameFromId(id);
		} else {
			integrationOrReconcilName = objLoggerManager.getIntegrationNameFromId(id);
		}
		// object of location
		LogFileLocationPath location = new LogFileLocationPath();
		// path of folder i.e ../logs/Integrations
		File logDir = new File(location.getIntegrationsFolderPath(logType));
		// get all integration files with starting name of integration ID
		File[] integrationFiles = logDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File folderName, final String filename) {
				return filename.startsWith(integrationOrReconcilName + LOG) || filename.startsWith(integrationOrReconcilName + LoggerManager.LogJobType.DELETE_MODE_FILENAME_POSTFIX + LOG);
			}
		});
		if (integrationFiles == null) {
			OpsHubLoggingUtil.debug(LOGGER, "Not able to rename the log file while deleting integration, renaming the log file to old as log file not found",null);
		} else {
			// rename all files
			for (int i = 0; i < integrationFiles.length; i++) {
				File oldFile = integrationFiles[i];
				// get configuration for the package where integration loggers are present
				LoggerConfig config = LoggerHandler.getLoggerConfiguration(AdminConstants.COM_OPSHUB);
				//Get the appender using name 
				Appender fileAppender = config.getAppenders().get(integrationOrReconcilName);
				if (fileAppender != null) {
					// close file and remove appender
					config.removeAppender(fileAppender.getName());
					LoggerHandler.closeAppender(fileAppender);
				} else {
					OpsHubLoggingUtil.debug(LOGGER,
							"FileAppender for old file : '" + integrationOrReconcilName + "' was not found.", null);
				}

				// get time stamp in milli seconds
				long millis = System.currentTimeMillis();
				// newName :- ../logs/Integrations/Old ItegrationName_timeStamp.log 
				String newName = logDir + File.separator + "Old " + integrationOrReconcilName + (oldFile.getName().startsWith(integrationOrReconcilName + LOG)?"":LoggerManager.LogJobType.DELETE_MODE_FILENAME_POSTFIX) + "_" + millis + LOG;
				// Rename file
				if (oldFile.renameTo(new File(newName))) {
					OpsHubLoggingUtil.debug(LOGGER, "File is renamed successfully.", null);
				} else {
					OpsHubLoggingUtil.debug(LOGGER, "Error occured while renaming the file.", null);
				}
			}
		}
	}

	/**
 * @param session hibernateSession
 * @param reconcileIds list of reconcile ids for which we want to fetch associated integration id
 * @return return list of integration found for given reconcile id, reconcile is associated with one integration
	 * 
 * */

	public List<Integer> getReconciliationIntegration(final Session session,
			final List<Integer> reconcileIds) {
		Query query = session.createQuery("select eaiIntegrations.integrationId from " + EAIReconciliations.class.getName() +  " where reconciliationId=:reconcileId");
		query.setParameterList("reconcileId", reconcileIds);
		if (query.list().size() > 0) {
			return query.list();
		}
		return null;

	}

	/**
	 * 
	 * @param id
	 * @param oldName
	 * @param loggerType
	 * @throws ORMException
	 * @throws LoggerManagerException
	 * @throws IOException
	 */
	private void changeLogFileName(final Integer id, final String oldFileName, final String loggerType)
			throws ORMException, LoggerManagerException, IOException {
		// // get configuration for the package where integration loggers are present to rename it
		LoggerConfig config = LoggerHandler.getLoggerConfiguration(AdminConstants.COM_OPSHUB);
		Appender fileAppender = config.getAppenders().get(loggerType + id);
		if (fileAppender != null) {
			// close file and remove appender
			config.removeAppender(fileAppender.getName());
			LoggerHandler.closeAppender(fileAppender);
		} else {
			OpsHubLoggingUtil.debug(LOGGER, "FileAppender for old file : '" + oldFileName + "' was not found.", null);
		}

		LogFileLocationPath location = new LogFileLocationPath();
		// ../logs/Integration
		File logDir = new File(location.getIntegrationsFolderPath(loggerType));
		// get old file from Integration folder and rename it
		final String oldName = oldFileName + LOG;
		final String deleteLogOldName = oldFileName + LoggerManager.LogJobType.DELETE_MODE_FILENAME_POSTFIX + LOG;
		File[] integrationFile = logDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File directory, final String fileName) {
				// keeping startsWith because files can be 1) log1.log, 2)
				// log1.log.1
				if (fileName.equals(oldName) || fileName.equals(deleteLogOldName)) {
					return true;
				} else if (fileName.startsWith(oldName)) {
					String fileNameTailingPart = fileName.substring(oldName.length() + 1);
					return StringUtils.isNumeric(fileNameTailingPart) ? true : false;
				} else if (fileName.startsWith(deleteLogOldName)) {
					String fileNameTailingPart = fileName.substring(deleteLogOldName.length() + 1);
					return StringUtils.isNumeric(fileNameTailingPart) ? true : false;
				}
				return false;
			}
		});

		// create full path for new renamed integration file i.e ../logs/Integration/newFileName.log
		LoggerManager objLoggerManager = new LoggerManager();
		String newIntegrationName;
		// check if it is reconciliation or integration
		if (LoggerManager.INTEGRATION.equals(loggerType)) {
			newIntegrationName = objLoggerManager.getIntegrationNameFromId(id);
		} else {
			newIntegrationName = objLoggerManager.getReconciliationNameFromId(id);
		}
		String newIntegrationFilePath = logDir + File.separator + newIntegrationName + LOG;
		String newDeleteLogFilePath = logDir + File.separator + newIntegrationName + LoggerManager.LogJobType.DELETE_MODE_FILENAME_POSTFIX + LOG;

		// rename old file to new file name
		if (integrationFile != null && integrationFile.length != 0) {
			for (File oldLogFile : integrationFile) {
				String oldLogFileName = oldLogFile.getName();
				// rename file to new name
				String newLogFileName=oldLogFileName.startsWith(deleteLogOldName) ? newDeleteLogFilePath : newIntegrationFilePath;
				// in case of log file is having .1 or .2 then newLogFileName
				// needs to be updated as per that scenario
				if (!oldName.equals(oldLogFileName) && !deleteLogOldName.equals(oldLogFileName)) {
					if(oldLogFileName.startsWith(deleteLogOldName)) {
						newLogFileName = new StringBuffer(newDeleteLogFilePath).append(oldLogFileName.substring(deleteLogOldName.length())).toString();
					} else {
						newLogFileName = new StringBuffer(newIntegrationFilePath).append(oldLogFileName.substring(oldName.length())).toString();
					}
				}
				boolean flag = oldLogFile.renameTo(new File(newLogFileName));
				if (flag) {
					OpsHubLoggingUtil.debug(LOGGER,
							"File name renamed sucessfully. New file is created at : '" + newIntegrationFilePath + "'",
							null);
				} else {
					OpsHubLoggingUtil.warn(LOGGER,
							"Not able to rename file at location : '" + newIntegrationFilePath + "'", null);
				}
			}
		} else {
			OpsHubLoggingUtil.debug(LOGGER, "File not found, back up of file is not creted.", null);
		}

		/**
		 * the file is not needed to be appended to the logger. This will be
		 * handled when the job is activated
		 */

	}

	private Map<String, Object> getMapFromListofMap(final List<Map<String, Object>> formData, final String dataKey) {
		Iterator<Map<String, Object>> itr = formData.iterator();
		while (itr.hasNext()) {
			Map<String, Object> map = itr.next();
			String keyStr = (String) map.get(KEY);
			if (keyStr.equals(dataKey)) {
				return map;
			}
		}
		return null;

	}

	public int getIntegrationIdFromReconcile(final Session session, final Integer reconcileId) {

		EAIReconciliations eaiReconciliation = (EAIReconciliations) session.load(EAIReconciliations.class.getName(), reconcileId);
		return eaiReconciliation.getEaiIntegrations().getIntegrationId();

	}

	public static int getReconcileRuleId(final Session session, final Integer integrationId) {

		String getMappingIdAssociatedWithUpdateEvent;
		String getRuleIdforMappingId;
		final Integer UPDATE_EVENT_TYPE_ID = 2;

		String getIntegrationsAssociated = " select eventProcessDefinitionId from " + EaiEventProcessDefinition.class.getName()+ WHERE_INTEGRATION_ID;

		Query query = session.createQuery(getIntegrationsAssociated);
		query.setParameter("id", integrationId);
		List<Integer> lst = query.list();
		for (Integer id : lst) {
			getMappingIdAssociatedWithUpdateEvent = " select transformationMapping.id from " + EaiEventTransformationMapping.class.getName()
					+ " where eventProcessDefinition.eventProcessDefinitionId =:id and eventType.eventTypeId = " + UPDATE_EVENT_TYPE_ID;
			Query getMappingAssociatedQuery = session.createQuery(getMappingIdAssociatedWithUpdateEvent);
			getMappingAssociatedQuery.setParameter("id", id);
			List<Integer> listOfMappingId = getMappingAssociatedQuery.list();

			if (listOfMappingId.size() > 0) {
				Integer mappingId = listOfMappingId.get(0);
				EAIMapperIntermidiateXML interMediateXml = session.get(EAIMapperIntermidiateXML.class, mappingId);
				getRuleIdforMappingId = "select distinct ruleScript.scriptId from RuleScriptAssociations where mappingScript.scriptId=:id";
				Query getRuleIdQuery = session.createQuery(getRuleIdforMappingId);
				EAIIntegrations integrations=session.get(EAIIntegrations.class, integrationId);
				getRuleIdQuery.setParameter("id", ConfigDirection.FORWARD_ID.equals(integrations.getDirection())?interMediateXml.getMappingIdEp1ToEp2():interMediateXml.getMappingIdEp2ToEp1());
				List<Integer> ruleIdList = getRuleIdQuery.list();
				if (ruleIdList != null && ruleIdList.size() > 0) {
					return ruleIdList.get(0);
				}
			}
		}
		return 0;


	}
	/*
	 * Returns true if current field is mandatory, and its parent if exists is mandatory.
	 * 
	 */
	private boolean isNotDependentMandatory(final List<Map<String, Object>> dataList,final Map<String, Object> formsMeta) {
		String dependentField = (String) formsMeta.get(UIConstants.DEPENDENTFIELDNAME);
		if(dependentField== null || dependentField.isEmpty()) return true;

		Map<String, Object> depFormsMeta = getFormsMetaByKey(dataList, dependentField);
		return Boolean.valueOf(depFormsMeta.get(UIConstants.MANDATORY).toString());
	}

	/*
	 * Fetch the forms metadata by key.
	 */
	private Map<String,Object> getFormsMetaByKey(final List<Map<String, Object>> dataList,
			final String key) {
		Map<String, Object> formsMeta = null;
		for (Map data : dataList) {
			if (data.get(UIConstants.KEY).equals(key)) {
				formsMeta = data;
				break;
			}
		}
		return formsMeta;
	}

	/*
	 * Skip fields, which do not need to be displayed at  Source configuration- source process context
	 */
	public static void skipInvalidSrcProcessContext(
			final List<Map<String, Object>> srcProcessCtxt) {
		Iterator<Map<String, Object>> srcCtxtItr = srcProcessCtxt.iterator();
		while (srcCtxtItr.hasNext()) {
			Map<String, Object> srcContextField = srcCtxtItr.next();
			if (SRC_JOB_CTXT_SKIP_LIST.contains(srcContextField.get(UIConstants.KEY))) {
				srcCtxtItr.remove();
			}
		}

	}

		public EAIIntegrationsBase createUpdateIntegration(final Session session, final BidirectionalIntegrationInfo integrationInfo,final EAIIntegrationGroup integrationGroup,
				final boolean isGraphQL, final IntegrationGroupInfo groupInfo) throws OpsHubBaseException, IOException {
		List<IntegrationValidationResult> validationResult = new ArrayList<IntegrationValidationResult>();
		AutomationDataHandler dataHandler = AutomationDataHandler.getInstance(false);
		TestCaseDataCarrier storedData = dataHandler.getDataObjectForTestcase(DUMMY_CREATE_UPDATE_INTEGRATION);
		if (groupInfo.getProjectMapping() == null || groupInfo.getProjectMapping().isEmpty()) {
			throw new OpsHubBaseException("012060", new Object[] { integrationInfo.getIntegrationName() }, null);
		}
		/*
		 * Skip validation if automation flag found. 
		 * Currently this flag will be set from jameleon
		 */
		if (storedData == null) {
			validationResult = validateIntegrationData(integrationInfo, isGraphQL, groupInfo);
			if (validationResult != null && !validationResult.isEmpty()) {
				throw new OpsHubBaseException(_017652, new String[] { validationResult.toString() }, null);
			}
		}
		Systems ep1System = session.get(Systems.class, integrationInfo.getEndpoint1());
		Systems ep2System = session.get(Systems.class, integrationInfo.getEndpoint2());
		// validate configuration and integration name
		validateConfiguration(ep1System, ep2System, integrationInfo);
		if (integrationInfo.getIntegrationId() == -1) {
			integrationGroup.setEndPoint1(ep1System);
			integrationGroup.setEndPoint2(ep2System);
		}
		EAIIntegrationsBase base = createUpdateIntegrationBase(integrationInfo, integrationGroup);
		Set<EAIIntegrations> integrations = new HashSet<>();
		// Based on direction proceed for integration, job instance and context
		if (integrationInfo.isForwardIntegrationConfigured()) {
			EAIIntegrations forwardIntegration = createUpdateUnidirectionalIntegration(session, integrationInfo, isGraphQL, groupInfo, ConfigDirection.FORWARD, base,ep1System,ep2System);
			integrations.add(forwardIntegration);
		}
		if (integrationInfo.isBackwardIntegrationConfigured()) {
			EAIIntegrations backwardIntegration = createUpdateUnidirectionalIntegration(session, integrationInfo, isGraphQL, groupInfo, ConfigDirection.BACKWARD, base,ep2System,ep1System);
			integrations.add(backwardIntegration);
		}
		// Setting the last updated on at time of saving the advance
		// configurations or saving without doing any change in the integration
		// of the integration group
		integrationGroup.setLastUpdatedOn(Calendar.getInstance().getTime());
		base.setIntegrations(integrations);
		Util.deleteTempFile(jiraWorkFlowFileName);
		jiraWorkFlowFileName = "";
		return base;
	}

		private EAIIntegrations createUpdateUnidirectionalIntegration(final Session session, final BidirectionalIntegrationInfo integrationInfo,
				final boolean isGraphQL, final IntegrationGroupInfo groupInfo,final ConfigDirection direction, final EAIIntegrationsBase base, final Systems sourceSystem, final Systems targetSystem ) throws OpsHubBaseException, IOException{
			OpsHubLoggingUtil.trace(LOGGER, "Creating unidirectional integration:" + integrationInfo + " for direction: " + direction, null);
			String integrationName =integrationInfo.getIntegrationName();
			String oldIntegrationName = null;
			String newIntegrationName ="";
			UnidirectionalIntegrationData integrationData = ConfigDirection.FORWARD.equals(direction) ?  integrationInfo.getForwardSettings() : integrationInfo.getBackwardSettings();
			
			if(!integrationName.equals(groupInfo.getGroupName())){
				integrationName =IntegrationGroupBO.constructIntegrationName(session, integrationInfo, direction.getDirection());
			}			
			if (integrationData.getUnidirectionalId() != null && integrationData.getUnidirectionalId() > 0) {
				oldIntegrationName = getOldIntegrationName(integrationData.getUnidirectionalId());
				newIntegrationName =integrationData.getUnidirectionalId() +"_" + integrationName;
			}
			boolean isNameUpdated = oldIntegrationName != null && !oldIntegrationName.equals(newIntegrationName);
			EAIIntegrations integration = saveUnidirectionalIntegration(base, integrationInfo,integrationData, sourceSystem,
					targetSystem, direction.getDirectionId(), isGraphQL,groupInfo,integrationName);
			saveCriteria(session, integrationData, integration,integrationInfo.getIntegrationId() != -1);
		// Copying required data from sec integration to merged primary
		copyRequiredIntegrationStorage(session, integration, integrationData);
			if (isNameUpdated) {
				// only the case where name is updated is handled.
				// if the integration is created. Activation of job will ensure the creation of logger.
				changeLogFileName(integration.getIntegrationId(), oldIntegrationName, LoggerManager.INTEGRATION);
			}			
			updateRecoSettingsFromIntegration(session, isNameUpdated, oldIntegrationName, integration,integrationData.getTargetLookUpContext(), isGraphQL);
			return integration;
		}
	
	/**
	 * This function copies the integration specific details for new integration
	 * from the base integration/from which it's being merged Purpose : So that
	 * the criteria context,Sync info etc get maintained with merged integration
	 * and sync and other functionality like Integration Sync Report works in
	 * same way as it was with secondary integration
	 * 
	 * @param session
	 * @param integration
	 *            [Integration of Create/Update]
	 * @param unidirectionalIntegrationData
	 */
	private void copyRequiredIntegrationStorage(final Session session, final EAIIntegrations integration,
			final UnidirectionalIntegrationData unidirectionalIntegrationData) {
		if (unidirectionalIntegrationData.getMergedFromIntegrationId() != null) {
			copyEntityContext(session, integration, unidirectionalIntegrationData);
			copyHistoryState(session, integration, unidirectionalIntegrationData);
			copyEventSyncInfo(session, integration, unidirectionalIntegrationData);
		}
	}

	/**
	 * This function will be called only when a user choose to merge two
	 * integrations and it migrate the entity context data if there is some for
	 * secondary integration
	 */
	private void copyEntityContext(final Session session, final EAIIntegrations integration,
			final UnidirectionalIntegrationData unidirectionalIntegrationData) {

		if (unidirectionalIntegrationData.getMergedFromIntegrationId() != null
				&& !unidirectionalIntegrationData.getCriteria().getConfigureCriteria().equals("0")) {
			Query<OIMEntityContext> getCriteria = session
					.createQuery(FROM + OIMEntityContext.class.getName() + WHERE_INTEGRATION_ID);
			getCriteria.setParameter("id", unidirectionalIntegrationData.getMergedFromIntegrationId());

			for (int i = getCriteria.getFirstResult(); i > -1; i += 100) {
				getCriteria.setFirstResult(i);
				getCriteria.setMaxResults(100);
				if (getCriteria.list().size() > 0) {
					List<OIMEntityContext> oimEntityContextList = getCriteria.list();

					for (OIMEntityContext oimEntityContext : oimEntityContextList) {
						OIMEntityContext newContext = new OIMEntityContext();
						newContext.setEntityInternalId(oimEntityContext.getEntityInternalId());
						newContext.setContextKey(oimEntityContext.getContextKey());
						newContext.setContextValue(oimEntityContext.getContextValue());
						newContext.setIntegration(integration);
						newContext.setEntityScopeId(oimEntityContext.getEntityScopeId());
						session.saveOrUpdate(newContext);
					}
				} else {
					break;
				}
			}
		}
	}

	/**
	 * @param session
	 * @param integration
	 * @param unidirectionalIntegrationData
	 */
	private void copyHistoryState(final Session session, final EAIIntegrations integration,
			final UnidirectionalIntegrationData unidirectionalIntegrationData) {
		if (unidirectionalIntegrationData.isHistoryStateUpdateRequired()) {
			int baseIntegrationId = session
					.get(EAIIntegrations.class, unidirectionalIntegrationData.getMergedFromIntegrationId())
					.getBaseIntegration().getIntegrationBaseId();
		Query<EAITimeboxEntityHistoryState> query = session.createQuery(
				FROM + EAITimeboxEntityHistoryState.class.getName() + " where baseIntegration.integrationBaseId =:id",
					EAITimeboxEntityHistoryState.class);
			query.setParameter("id", unidirectionalIntegrationData.getMergedFromIntegrationId());
			for (int i = query.getFirstResult(); i > -1; i += 100) {
				query.setFirstResult(i);
				query.setMaxResults(100);
				if (query.list().size() > 0) {
					List<EAITimeboxEntityHistoryState> timeBoxEntityStates = query.getResultList();
					for (EAITimeboxEntityHistoryState timeBoxEntityState : timeBoxEntityStates) {
						OIMEntityHistoryState entityHistoryState = timeBoxEntityState.getHistoryState();
						OIMEntityHistoryState historyState = new OIMEntityHistoryState();
						historyState.setEntityInfo(entityHistoryState.getEntityInfo());
						historyState.setTargetSystem(entityHistoryState.getTargetSystem());
						historyState.setOldState(entityHistoryState.getOldState());
						historyState.setAttachmentMapping(entityHistoryState.getAttachmentMapping());
						session.saveOrUpdate(historyState);

						EAITimeboxEntityHistoryState newTimeBoxEntityHistoryState = new EAITimeboxEntityHistoryState(
								historyState, integration.getBaseIntegration());
						session.saveOrUpdate(newTimeBoxEntityHistoryState);

					}
					if (query.list().size() < 100) {
						break;
					}
				} else {
					break;
				}
			}
		}
	}

	/**
	 * This method copies the Event Sync Info Table details from the
	 * MergedFromIntegrationId, to the integration being Created/updated
	 * 
	 * @param session
	 * @param integration
	 * @param unidirectionalIntegrationData
	 */
	private void copyEventSyncInfo(final Session session, final EAIIntegrations integration,
			final UnidirectionalIntegrationData unidirectionalIntegrationData) {

		Integer mergedFromIntId = unidirectionalIntegrationData.getMergedFromIntegrationId();

		if (mergedFromIntId != null) {

			org.hibernate.query.Query<OIMEntitySyncInfo> query = getQueryForEventSyncInfo(session, mergedFromIntId);

			int startIndex = 0;
			final int pageSize = 100;
			// Copy the table data in batch of 100
			while (true) {
				// Get the Page result
				query.setFirstResult(startIndex);
				query.setMaxResults(pageSize);
				List<OIMEntitySyncInfo> entitySyncInfoList = query.getResultList();

				// Copy all the EntitySyncInfo of existing/base integration with
				// new integration details/Id
				if (entitySyncInfoList != null && !entitySyncInfoList.isEmpty()) {
					for (OIMEntitySyncInfo entitySyncInfo : entitySyncInfoList) {
						OIMEntitySyncInfo newSyncInfo = new OIMEntitySyncInfo();
						newSyncInfo.setDestinationStepId(entitySyncInfo.getDestinationStepId());
						newSyncInfo.setDestinationTransactionId(entitySyncInfo.getDestinationTransactionId());
						newSyncInfo.setDestinationUpdateTime(entitySyncInfo.getDestinationUpdateTime());
						newSyncInfo.setEntityInfoId(entitySyncInfo.getEntityInfoId());
						newSyncInfo.setIntegration(integration);
						newSyncInfo.setOpshubUpdateTime(entitySyncInfo.getOpshubUpdateTime());
						newSyncInfo.setSourceUpdateTime(entitySyncInfo.getSourceUpdateTime());
						newSyncInfo.setWorkflowStepId(entitySyncInfo.getWorkflowStepId());
						newSyncInfo.setSourceSyncStatus(entitySyncInfo.getSourceSyncStatus());
						newSyncInfo.setTargetSyncStatus(entitySyncInfo.getTargetSyncStatus());
						session.saveOrUpdate(newSyncInfo);
					}
					if (entitySyncInfoList.size() < pageSize) {
						break;
					} else {
						startIndex = startIndex + pageSize;
					}
				} else {
					OpsHubLoggingUtil.debug(LOGGER,
							"Event Sync info list of MergedFromIntegrationId " + mergedFromIntId + " is null/empty",
							null);
					break;
				}
			}
		}
	}

	/**
	 * Construct the Query to retrive base integration's event sync info
	 * 
	 * @param session
	 * @param baseIntId
	 * @return
	 */
	private org.hibernate.query.Query getQueryForEventSyncInfo(final Session session, final Integer baseIntId) {
		org.hibernate.query.Query<OIMEntitySyncInfo> query = null;
		if (baseIntId != null) {
			CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
			CriteriaQuery<OIMEntitySyncInfo> criteriaQuery = criteriaBuilder.createQuery(OIMEntitySyncInfo.class);

			Root entitySyncInfoRoot = criteriaQuery.from(OIMEntitySyncInfo.class);
			// Fetch entity sync info for given base integration id
			Predicate integrationFilter = criteriaBuilder.equal(entitySyncInfoRoot.get(OIMEntitySyncInfo_.integration),
					baseIntId);

			// Form the QueryInteger baseIntId
			criteriaQuery.select(entitySyncInfoRoot).where(integrationFilter);
			query = session.createQuery(criteriaQuery);
		}
		return query;
	}
	private List<IntegrationValidationResult> validateProjectMappingDirection(
			final List<ProjectMappingContext> projectMappingList) {
		List validationResult = new ArrayList<>();
		for (ProjectMappingContext projectMapping : projectMappingList) {
			if (projectMapping.getDirection() == null || projectMapping.getDirection().getDirection() == null) {
				validationResult.add(new IntegrationValidationResult("1025",
						new Object[] { projectMapping.getEp1Project(), projectMapping.getEp2Project() }));
			}
		}
		return validationResult;
	}

	private List<IntegrationValidationResult> validateIntegrationData(final BidirectionalIntegrationInfo integrationInfo,final boolean isGraphQL, final IntegrationGroupInfo groupInfo) throws OpsHubBaseException {
		OpsHubLoggingUtil.trace(LOGGER, "Validating integration data", null);
		List<IntegrationValidationResult> validationResult = firstLevelValidation(integrationInfo);
		if (!(validationResult == null || validationResult.isEmpty())) {
			return validationResult;
		}

		HasMetadata ep1MetaDataImpl = MetadataImplFactory.getMetadataImplClass(session, integrationInfo.getEndpoint1());
		HasMetadata ep2MetaDataImpl = MetadataImplFactory.getMetadataImplClass(session, integrationInfo.getEndpoint2());

		validationResult = validateProjectContext(integrationInfo, ep1MetaDataImpl, ep2MetaDataImpl);
		if (!(validationResult == null || validationResult.isEmpty())) {
			return validationResult;
		}

		List<String> ep1Projects = EaiUtility.getProjectList(groupInfo.getProjectMapping(), true);
		List<String> ep2Projects = EaiUtility.getProjectList(groupInfo.getProjectMapping(), false);

		IntegrationValidator executor = new IntegrationValidator();
		validationResult = validateUnidrectionalFlow(integrationInfo, groupInfo.getProjectMapping());
		if(validationResult!=null && !validationResult.isEmpty()){
			return validationResult;
		}
		List validators = new ArrayList<>();
		if (integrationInfo.getForwardSettings() != null) {
			validators.add(new UniDirectionalIssueTypeValidator(integrationInfo.getForwardSettings().getSourceContext().getIssueType(), integrationInfo.getForwardSettings().getTargetContext().getIssueType(), ep1Projects, ep2Projects, integrationInfo.getEndpoint1(), integrationInfo.getEndpoint2(), ep1MetaDataImpl, ep2MetaDataImpl,ConfigDirection.FORWARD_ID));
		}
		if (integrationInfo.getBackwardSettings() != null) {
			validators.add(new UniDirectionalIssueTypeValidator(integrationInfo.getBackwardSettings().getSourceContext().getIssueType(), integrationInfo.getBackwardSettings().getTargetContext().getIssueType(), ep2Projects, ep1Projects, integrationInfo.getEndpoint2(), integrationInfo.getEndpoint1(), ep2MetaDataImpl, ep1MetaDataImpl,ConfigDirection.BACKWARD_ID));	
		}

		validationResult = executor.submitValidators(validators, 20);

		if (!(validationResult == null || validationResult.isEmpty())) {
			return validationResult;
		}
		String ep1IssueType = getEndPoint1IssueType(integrationInfo);
		String ep2IssueType = getEndPoint2IssueType(integrationInfo);
		boolean isEp1RemoteLinkEnabled = false;
		boolean isEp2RemoteLinkEnabled = false;
		boolean isEp1RemoteIdEnabled = false;
		boolean isEp2RemoteIdEnabled = false;
		boolean isEp1SyncFieldEnabled = false;
		boolean isEp2SyncFieldEnabled = false;

		Systems ep1 = session.get(Systems.class, integrationInfo.getEndpoint1());
		Systems ep2 = session.get(Systems.class, integrationInfo.getEndpoint2());
		if (integrationInfo.getForwardSettings() != null) {
			RemoteLinkage  forwardContext = integrationInfo.getForwardSettings().getRemoteLinkContext();
			IntegrationFormValidator advanceFormDataValidator = getFormValidator(integrationInfo.getForwardSettings(), ep1MetaDataImpl, ep2MetaDataImpl,
					ep1Projects, ep2Projects, ep1, ep2, isGraphQL);
			validationResult.addAll(advanceFormDataValidator.validate());
			if(forwardContext!=null){
				isEp1RemoteLinkEnabled = StringUtils.isNotEmpty(forwardContext.getSourceLinkField());
				isEp2RemoteLinkEnabled = StringUtils.isNotEmpty(forwardContext.getTargetLinkField());
				isEp1RemoteIdEnabled = StringUtils.isNotEmpty(forwardContext.getSourceEntityIdField());
				isEp2RemoteIdEnabled = StringUtils.isNotEmpty(forwardContext.getTargetEntityIdField());
			}
			if(integrationInfo.getForwardSettings().getSourceContext()!=null)
				isEp1SyncFieldEnabled = StringUtils.isNotEmpty(integrationInfo.getForwardSettings().getSourceContext().getSyncFieldName());
		}
		if (integrationInfo.getBackwardSettings() != null) {
			RemoteLinkage  backwardContext = integrationInfo.getBackwardSettings().getRemoteLinkContext();
			IntegrationFormValidator advanceFormDataValidator = getFormValidator(integrationInfo.getBackwardSettings(), ep2MetaDataImpl, ep1MetaDataImpl,
					ep2Projects, ep1Projects, ep2, ep1, isGraphQL);
			validationResult.addAll(advanceFormDataValidator.validate());
			if(backwardContext!=null){
				isEp1RemoteLinkEnabled = isEp1RemoteLinkEnabled || StringUtils.isNotEmpty(backwardContext.getTargetLinkField());
				isEp2RemoteLinkEnabled = isEp2RemoteLinkEnabled || StringUtils.isNotEmpty(backwardContext.getSourceLinkField());
				isEp1RemoteIdEnabled = isEp1RemoteIdEnabled || StringUtils.isNotEmpty(backwardContext.getSourceEntityIdField());
				isEp2RemoteIdEnabled = isEp2RemoteIdEnabled ||StringUtils.isNotEmpty(backwardContext.getTargetEntityIdField());
			}
			if(integrationInfo.getBackwardSettings().getSourceContext()!=null)
				isEp2SyncFieldEnabled = StringUtils.isNotEmpty(integrationInfo.getBackwardSettings().getSourceContext().getSyncFieldName());
		}
		IntegrationGroupContext groupContext = groupInfo.getGroupContext();
		if (groupContext != null) {
			Map<String, Object> ep1ProcessContext = getProcessContextMap(ep1IssueType,
					ep1MetaDataImpl.getProjectKeyName(), ep1Projects);
			Map<String, Object> ep2ProcessContext = getProcessContextMap(ep2IssueType,
					ep2MetaDataImpl.getProjectKeyName(), ep2Projects);
			validationResult.addAll(validateGroupContextData(groupContext, ep1ProcessContext, ep2ProcessContext, ep1,
					ep2, !isEp1RemoteLinkEnabled, !isEp1RemoteIdEnabled,
					!isEp2RemoteLinkEnabled,!isEp2RemoteIdEnabled,!isEp1SyncFieldEnabled,!isEp2SyncFieldEnabled));
		}
		return validationResult;
	}

	/**
	 * This will throw error is entity mapping is enabled for unidrectional flow and project mapping has not been provided for the same
	 * @param integrationInfo
	 * @param projectMapping
	 * @return
	 */
	private List<IntegrationValidationResult>  validateUnidrectionalFlow(final BidirectionalIntegrationInfo integrationInfo, final List<ProjectMappingContext> projectMapping){
		List<IntegrationValidationResult> validationResult = new ArrayList<IntegrationValidationResult>();
		if(integrationInfo.getForwardSettings()!=null && !EaiUtility.isForwardProjectMapping(projectMapping) ){
			validationResult.add(new IntegrationValidationResult("1016", new Object[]{"Forward"}));
		}
		if(integrationInfo.getBackwardSettings()!=null && !EaiUtility.isBackwardProjectMapping(projectMapping) ){
			validationResult.add(new IntegrationValidationResult("1016", new Object[]{"Backward"}));
		}
		return validationResult;

	}
	/**
	 * This will construct processcontext map based on given parameters
	 * 
	 * @param issueType
	 * @param projectKey
	 * @param epProjects
	 * @return
	 */
	public Map<String, Object> getProcessContextMap(final String issueType, final String projectKey,
			final List<String> epProjects) {
		Map<String, Object> epProcessContext = new HashMap<>();
		epProcessContext.put(EAIJobConstants.ISSUETYPE, issueType);
		epProcessContext.put(projectKey, epProjects);
		return epProcessContext;
	}

	/**
	 * This will get EndPoint 1 issue type for given integration
	 * 
	 * @param integrationInfo
	 * @return
	 */
	public String getEndPoint1IssueType(final BidirectionalIntegrationInfo integrationInfo) {
		String epIssueType = "";
		if (integrationInfo.getForwardSettings() != null) {
			epIssueType = integrationInfo.getForwardSettings().getSourceContext().getIssueType();
		} else if (integrationInfo.getBackwardSettings() != null) {
			epIssueType = integrationInfo.getBackwardSettings().getTargetContext().getIssueType();
		}
		return epIssueType;
	}

	/**
	 * This will get EndPoint 2 issue type for given integration
	 * 
	 * @param integrationInfo
	 * @return
	 */
	public String getEndPoint2IssueType(final BidirectionalIntegrationInfo integrationInfo) {
		String epIssueType = "";
		if (integrationInfo.getForwardSettings() != null) {
			epIssueType = integrationInfo.getForwardSettings().getTargetContext().getIssueType();
		} else if (integrationInfo.getBackwardSettings() != null) {
			epIssueType = integrationInfo.getBackwardSettings().getSourceContext().getIssueType();
		}
		return epIssueType;
	}

	private List<IntegrationValidationResult> validateGroupContextData(final IntegrationGroupContext groupContext,final Map<String, Object> ep1ProcessContext, final Map<String, Object> ep2ProcessContext
			, final Systems ep1Info, final Systems ep2Info, final boolean validateEp1Link, final boolean validateEp1Id,
			final boolean validateEp2Link, final boolean validateEp2Id, final boolean validateEp1Sync,
			final boolean validateEp2Sync) throws OpsHubBaseException {

		List<IntegrationValidationResult> validationResult = new ArrayList<>();

		// if endpoint field mapped then only validate
		if (groupContext.isEndPointFieldMapped()) {
			MapperBO mapperBo = new MapperBO(session);

			List<FieldsMeta> ep1FieldMetadata = mapperBo.getFieldsMetadata(ep1Info.getSystemId(), ep1ProcessContext,
					false, false);
			List<FieldsMeta> ep2FieldMetadata = mapperBo.getFieldsMetadata(ep2Info.getSystemId(), ep2ProcessContext,
					false, false);
			
			String ep1IssueType = String.valueOf(ep1ProcessContext.get("issueType"));
			String ep2IssueType = String.valueOf(ep2ProcessContext.get("issueType"));
			HasMetadata ep1Metabase = MetadataImplFactory.getMetadataImplClass(session, ep1Info);
			String ep1ProjectKey = ep1Metabase.getProjectKeyName();
			String ep1Project = Joiner.on(',').join((List<String>)ep1ProcessContext.get(ep1ProjectKey));
			HasMetadata ep2Metabase = MetadataImplFactory.getMetadataImplClass(session, ep2Info);
			String ep2ProjectKey = ep2Metabase.getProjectKeyName();
			String ep2Project = Joiner.on(',').join((List<String>)ep2ProcessContext.get(ep2ProjectKey));
			String ep1IssueDisplay = EaiUtility.getEntityDisplayName(session, ep1Info.getSystemId(), ep1Project,
					ep1IssueType);
			String ep2IssueDisplay = EaiUtility.getEntityDisplayName(session, ep2Info.getSystemId(), ep2Project,
					ep2IssueType);
			
			if (validateEp1Id && !isValidField(ep1FieldMetadata, groupContext.getEp1EntityIdField())) {
				Object[] errorArgs = { ep1Info.getDisplayName() + "Entity Id Field Name",
						groupContext.getEp1EntityIdField(), ep1IssueDisplay };
				validationResult.add(new IntegrationValidationResult(_0022, errorArgs));
			}
			if (validateEp1Link && !isValidField(ep1FieldMetadata, groupContext.getEp1LinkField())) {
				Object[] errorArgs = { ep1Info.getDisplayName() + "Link Field Name", groupContext.getEp1LinkField(),
						ep1IssueDisplay };
				validationResult.add(new IntegrationValidationResult(_0022, errorArgs));
			}
			if (validateEp1Sync && !isValidField(ep1FieldMetadata, groupContext.getEp1SyncFieldName())) {
				Object[] errorArgs = { ep1Info.getDisplayName() + "Sync Confirmation Field Name",
						groupContext.getEp1SyncFieldName(), ep1IssueDisplay };
				validationResult.add(new IntegrationValidationResult(_0022, errorArgs));
			}
			if (validateEp2Id && !isValidField(ep2FieldMetadata, groupContext.getEp2EntityIdField())) {
				Object[] errorArgs = { ep2Info.getDisplayName() + "Entity Id Field Name",
						groupContext.getEp2EntityIdField(), ep2IssueDisplay };
				validationResult.add(new IntegrationValidationResult(_0022, errorArgs));
			}
			if (validateEp2Link && !isValidField(ep2FieldMetadata, groupContext.getEp2LinkField())) {
				Object[] errorArgs = { ep2Info.getDisplayName() + "Link Field Name", groupContext.getEp2LinkField(),
						ep2IssueDisplay };
				validationResult.add(new IntegrationValidationResult(_0022, errorArgs));
			}
			if (validateEp2Sync && !isValidField(ep2FieldMetadata, groupContext.getEp2SyncFieldName())) {
				Object[] errorArgs = { ep2Info.getDisplayName() + "Sync Confirmation Field Name",
						groupContext.getEp2SyncFieldName(), ep2IssueDisplay };
				validationResult.add(new IntegrationValidationResult(_0022, errorArgs));
			}
		}
		return validationResult;
	}

	private boolean isValidField(final List<FieldsMeta> fieldMetadata, final String value) {
		if(StringUtils.isEmpty(value)) return true;
		for (FieldsMeta fieldsMeta : fieldMetadata) {
			if (StringUtils.isNotEmpty(fieldsMeta.getDisplayName()) && fieldsMeta.getDisplayName().equals(value)) {
				return true;
			}
		}
		return false;

	}

	private IntegrationFormValidator getFormValidator(final UnidirectionalIntegrationData unidirectionalIntegrationData,
			final HasMetadata sourceMetaDataImpl,final HasMetadata targetMetaDataImpl,final List<String> sourceProjects,
			final List<String> targetProjects, final Systems sourceSystemId, final Systems targetSystemId,
			final boolean isGraphQl) throws OpsHubBaseException {

		HashMap<String, Object> source = new HashMap<>();
		HashMap<String, Object> target = new HashMap<>();

		source.put(EAIJobConstants.ISSUETYPE, unidirectionalIntegrationData.getSourceContext().getIssueType());
		target.put(EAIJobConstants.ISSUETYPE, unidirectionalIntegrationData.getTargetContext().getIssueType());

		source.put(sourceMetaDataImpl.getProjectKeyName(), sourceProjects);

		target.put(targetMetaDataImpl.getProjectKeyName(), targetProjects);

		EAIIntegrations eaiIntegrations = null;
		if (unidirectionalIntegrationData.getUnidirectionalId() != -1) {
			eaiIntegrations = session.load(EAIIntegrations.class, unidirectionalIntegrationData.getUnidirectionalId());
		}

		IntegrationContextDataProvider dataProvider = getContextMetaDataProvider(
				eaiIntegrations, sourceSystemId, targetSystemId, source, target);
		return new IntegrationFormValidator(unidirectionalIntegrationData, dataProvider,
				sourceMetaDataImpl.getAllProjectKeyName(), isGraphQl);
	}

	private IntegrationContextDataProvider getContextMetaDataProvider(final EAIIntegrations integrationInfo,
			final Systems sourceSystem, final Systems targetSystem, final HashMap<String, Object> source,
			final HashMap<String, Object> target) throws OpsHubBaseException {
		Integer integrationId = -1;
		if (integrationInfo != null) {
			integrationId = integrationInfo.getIntegrationId();
		}
		HashMap<String, List<Map<String, Object>>> allContextAdvacnceData = getAdvanceConfigurationData(sourceSystem,
				targetSystem, integrationId, false, source, target, false);
		HashMap<String, FieldValue> initValues = new HashMap<String, FieldValue>();
		List<Map<String, Object>> jobContext = getJobContext(session, sourceSystem.getSystemId(), integrationInfo,
				false, initValues);
		List<Map<String, Object>> processContext = (List<Map<String, Object>>) getProcessContextParameters(session,
				targetSystem, true, integrationId, false, false, null).get(UIConstants.PROCESS_CONTEXT);
		IntegrationContextDataProvider dataProvider = new IntegrationContextDataProvider(allContextAdvacnceData,
				jobContext, processContext);
		return dataProvider;
	}

	/**
	 * @param integrationInfo
	 * @param targetMetaDataImpl
	 * @param sourceMetaDataImpl
	 * @return
	 * @throws OpsHubBaseException
	 */
	private List<IntegrationValidationResult> validateProjectContext(final BidirectionalIntegrationInfo integrationInfo,final  HasMetadata sourceMetaDataImpl,final  HasMetadata targetMetaDataImpl) throws OpsHubBaseException {	

		IntegrationValidator executor = new IntegrationValidator();

		List validators = new ArrayList<>();
		validators.add(new ProjectContextValidator(integrationInfo.getGroupInfo().getProjectMapping(), true, sourceMetaDataImpl));
		validators.add(new ProjectContextValidator(integrationInfo.getGroupInfo().getProjectMapping(), false, targetMetaDataImpl));
		List result = executor.submitValidators(validators, 10);

		result.addAll(this.validateProjectMappingDirection(integrationInfo.getGroupInfo().getProjectMapping()));
		return result;
	}

	/**
	 * @param integrationInfo
	 * @param validationResult
	 */
	private List<IntegrationValidationResult> firstLevelValidation(final BidirectionalIntegrationInfo integrationInfo) {
		List<IntegrationValidationResult> validationResult = new ArrayList<>();
		// 1. Validate IntegrationId
		if(integrationInfo.getIntegrationId()!=null && integrationInfo.getIntegrationId()!=-1 &&!EaiUtility.isValidId(session, EAIIntegrationsBase.class, integrationInfo.getIntegrationId())){	
			Object[] errorArgs = {"bidirectional integration", integrationInfo.getIntegrationId()};
			validationResult.add(new IntegrationValidationResult("0003",new Object[]{"Bidirectional integration", integrationInfo.getIntegrationId()}));
		}

		// 2. Validate End Points
		if (!EaiUtility.isValidId(session, Systems.class, integrationInfo.getEndpoint1())) {
			validationResult.add(new IntegrationValidationResult("0004",new Object[]{BidirectionalValidationConstants.SYSTEM1, integrationInfo.getEndpoint1()}));
		}

		if (!EaiUtility.isValidId(session, Systems.class, integrationInfo.getEndpoint2())) {
			validationResult.add(new IntegrationValidationResult("0004",new Object[]{BidirectionalValidationConstants.SYSTEM2, integrationInfo.getEndpoint1()}));
		}

		return validationResult;
	}

	public void createOrUpdateProjectMapping(final IntegrationGroupInfo integrationInfo,
			final EAIProjectMappingBase mappingRefBase, final EAIIntegrationGroup intGroup) throws OpsHubBaseException {

		HasMetadata metadataImplSource = MetadataImplFactory.getMetadataImplClass(session, integrationInfo.getEndPoint1());
		HasMetadata metadataImplTarget = MetadataImplFactory.getMetadataImplClass(session, integrationInfo.getEndPoint2());

		List<ProjectMeta> ep1projectMeta = metadataImplSource.getProjectsMeta();
		List<ProjectMeta> ep2projectMeta = metadataImplTarget.getProjectsMeta();

		Map<String, String> ep1projectsMetaMap = getProjectMetaMap(ep1projectMeta);
		Map<String, String> ep2projectsMetaMap = getProjectMetaMap(ep2projectMeta);

		final List<ProjectMappingContext> prjMapping = integrationInfo.getProjectMapping();
		OpsHubLoggingUtil.debug(LOGGER, "Project mapping received : " + prjMapping, null);
		HashMap<String, EAIProjectMappings> currentMappings = new HashMap<>();
		// if project mapping is specified
		if (!prjMapping.isEmpty()) {

			Iterator<ProjectMappingContext> prjMappingItr = prjMapping.iterator();
			while (prjMappingItr.hasNext()) {

				ProjectMappingContext prjMappingCtxt = prjMappingItr.next();
				ConfigDirection direction = prjMappingCtxt.getDirection();
				String ep1ProjectKey = prjMappingCtxt.getEp1Project();
				String ep2ProjectKey = prjMappingCtxt.getEp2Project();

				String ep1ProjectDisplayName = Constants.OH_PROJECT_KEYS.contains(ep1ProjectKey) ? PROJECT_KEY_WITH_DISPLAY_NAME.get(ep1ProjectKey) : ep1projectsMetaMap.get(ep1ProjectKey);
				String ep2ProjectDisplayName = Constants.OH_PROJECT_KEYS.contains(ep2ProjectKey)? PROJECT_KEY_WITH_DISPLAY_NAME.get(ep2ProjectKey) : ep2projectsMetaMap.get(ep2ProjectKey);

				EAIProjectMappings projectMapping = constructEAIProjectMapping(ep1ProjectKey, ep2ProjectKey,
						ep1ProjectDisplayName, ep2ProjectDisplayName, direction.getDirectionId(), mappingRefBase);
				currentMappings.put(ep1ProjectKey + "-" + ep2ProjectKey, projectMapping);

			}

		}
		HashMap<String, EAIProjectMappings> existingMappings = getExistingProjectMappings(mappingRefBase.getId());
		saveProjectMapping(session, existingMappings, currentMappings, intGroup);

	}

	private Map<String, String> getProjectMetaMap(final List<ProjectMeta> projectMetaList) {
		Map<String, String> projectsMetaMap = new HashMap<>();

		if (projectMetaList != null && !projectMetaList.isEmpty()) {
			for (ProjectMeta projectMeta : projectMetaList) {
				projectsMetaMap.put(projectMeta.getProjectKey(), projectMeta.getProjectDisplayName());
			}
		}
		return projectsMetaMap;

	}

	private EAIProjectMappings constructEAIProjectMapping(final String ep1ProjectKey, final String ep2ProjectKey,
			final String ep1ProjectDisplay, final String ep2ProjectDisplay,
				final Integer direction,final EAIProjectMappingBase mappingRefBase){
		EAIProjectMappings projectMapping = new EAIProjectMappings();
		projectMapping.setEp1ProjectKey(ep1ProjectKey);
		projectMapping.setEp2ProjectKey(ep2ProjectKey);
		projectMapping.setEp1ProjectName(ep1ProjectDisplay);
		projectMapping.setEp2ProjectName(ep2ProjectDisplay);
		projectMapping.setDirection(direction);
		projectMapping.setProjectMappingRefBase(mappingRefBase);
		return projectMapping;
	}

	private HashMap<String, EAIProjectMappings> getExistingProjectMappings(final Integer projectMappingRefId) {
		OpsHubLoggingUtil.trace(LOGGER, "Getting existing project mappings for project mapping reference id:" + projectMappingRefId, null);
		Query mappingQry = session.createQuery(FROM + EAIProjectMappings.class.getName() + " where projectMappingRefBase.id=:id");
		mappingQry.setParameter("id", projectMappingRefId);
		List<EAIProjectMappings> mappings = mappingQry.getResultList();
		HashMap<String, EAIProjectMappings> existingMappings = new HashMap<>();
		for (EAIProjectMappings projectMapping : mappings) {
			existingMappings.put(projectMapping.getEp1ProjectKey()+"-"+projectMapping.getEp2ProjectKey(), projectMapping);
		}
		return existingMappings;
	}

	private void saveProjectMapping(final Session session, final HashMap<String, EAIProjectMappings> existingMappings,
			final HashMap<String, EAIProjectMappings> currentMappings, final EAIIntegrationGroup intGroup) {
		List<String> operated = new ArrayList<>();
		Iterator<Entry<String, EAIProjectMappings>> it = currentMappings.entrySet().iterator();
		OpsHubLoggingUtil.debug(LOGGER, "Project mapping existing : " + existingMappings, null);

		while (it.hasNext()) {
			Map.Entry<String, EAIProjectMappings> mapping = it.next();
			// Create new project mapping
			if (!existingMappings.containsKey(mapping.getKey())) {
				OpsHubLoggingUtil.debug(LOGGER, "Project mapping added : " + mapping.getKey(), null);
				session.saveOrUpdate(mapping.getValue());
			}
			// Compare and update
			else {
				EAIProjectMappings existingMapping = existingMappings.get(mapping.getKey());
				EAIProjectMappings mappingToUpdate = mapping.getValue();
				// Update the mapping
				EAIProjectMappings updateMapping = session.get(EAIProjectMappings.class, existingMapping.getMappingId());
				if(!existingMapping.getEp1ProjectName().equals(mappingToUpdate.getEp1ProjectName()) ||!existingMapping.getEp2ProjectName().equals(mappingToUpdate.getEp2ProjectName())
						|| !existingMapping.getDirection().equals(mappingToUpdate.getDirection())) {
					updateMapping.setEp1ProjectName(mappingToUpdate.getEp1ProjectName());
					updateMapping.setEp2ProjectName(mappingToUpdate.getEp2ProjectName());
					updateMapping.setDirection(mappingToUpdate.getDirection());
				}
				session.saveOrUpdate(updateMapping);
				OpsHubLoggingUtil.debug(LOGGER, "Project mapping not changed : " + mapping.getKey(), null);
				operated.add(mapping.getKey());
			}
			/*
			 *  on change of project mapping, last update time should be updated to make sure that parent entity is getting changed
			 *  This helps in two ways, it increments the version of parent as well as update the modified time
			 */
			
			intGroup.setLastUpdatedOn(Calendar.getInstance().getTime());
		}

		//For all the remaining mappings , which are in database but not in current request , delete them

		Iterator<Entry<String, EAIProjectMappings>> itr = existingMappings.entrySet().iterator();
		while (itr.hasNext()) {
			Map.Entry<String, EAIProjectMappings> mappingToDelete = itr.next();
			if (!operated.contains(mappingToDelete.getKey())) {
				removeProjectMapping(mappingToDelete.getValue().getMappingId());
				OpsHubLoggingUtil.debug(LOGGER, "Project mapping removed : " + mappingToDelete.getKey(), null);
			}
		}

	}

	private void removeProjectMapping(final int projectMappingId) {
		EAIProjectMappings mappingToDelete = session.get(EAIProjectMappings.class, projectMappingId);
		session.delete(mappingToDelete);
	}

	public void validateFeaturesForLicenseUsage(final EAIIntegrations eaiIntegrations)
			throws LicenseException, OIMEditionException {
		LicenseEditionVerifier licEditionVerifier = new LicenseEditionVerifier(session);		
		licEditionVerifier.verifyExpiredFeaturesOfIntegration(eaiIntegrations);
	}
	
	private static final class DeleteJobConfiguration {
		@Getter
		@Setter
		private boolean configureDeleteJob;
		
		@Getter
		@Setter
		private String deleteJobSchedule;
		
		@Getter
		@Setter
		private String deleteJobPollingType;
		
		@Getter
		@Setter
		private String deleteJobPollingDate;
	}
	
	private DeleteJobConfiguration getDeleteJobConfiguration(final UnidirectionalIntegrationData unidirectionalInfo) {
		DeleteJobConfiguration configuration = new DeleteJobConfiguration();
		if(unidirectionalInfo.getTargetContext() != null) {
			List<ContextData> contextData = unidirectionalInfo.getTargetContext().getAdditionalData();
			if(contextData != null) {
				for(ContextData context : contextData) {
					if (Constants.DETECT_SOURCE_DELETE_FIELD.equals(context.getKey()) && Constants.ENABLED_DETECT_SOURCE_DELETE.equals(context.getValue())) {
						configuration.setConfigureDeleteJob(true);
					}else if (Constants.DELETE_JOB_SCHEDULE.equals(context.getKey())) {
						configuration.setDeleteJobSchedule(context.getValue());
					} else if (Constants.DELETE_JOB_POLLING_TYPE.equals(context.getKey())) {
						configuration.setDeleteJobPollingType(context.getValue());
					} else if (Constants.DELETE_JOB_POLLING_DATE.equals(context.getKey())) {
						configuration.setDeleteJobPollingDate(context.getValue());
					}
				}
			}
		}
		return configuration;
	}


	private EAIIntegrations saveUnidirectionalIntegration(final EAIIntegrationsBase base,
			final BidirectionalIntegrationInfo integrationInfo, final UnidirectionalIntegrationData unidirectionalInfo,
			final Systems sourceSystem, final Systems destSystem, final Integer direction, final boolean isGraphQL,
			final IntegrationGroupInfo groupInfo, final String integrationName) throws OpsHubBaseException {
		IntegrationGroupContext groupContext = groupInfo.getGroupContext();
		// get source entity
		EntityMeta sourceEntity = getSourceEntityMeta(sourceSystem, unidirectionalInfo.getSourceContext().getIssueType(),groupInfo.getProjectMapping(), direction);
		// create/ update and save job instance
		JobInstance instance = null;
		JobInstance deleteJobInstance = null;
		EAIIntegrations intgeration = null;
		if (integrationInfo.getIntegrationId() != null && integrationInfo.getIntegrationId() > 0) {
			intgeration = EAIIntegrationsBase.getIntegration(direction, integrationInfo.getIntegrationId(), session);
			if (intgeration != null) {
				instance = intgeration.getSourceJobInstance();
				deleteJobInstance = intgeration.getDeleteJobInstance();
			}
		}
		String globalSchedule = "";
		if (groupContext != null) {
			globalSchedule = groupContext.getGlobalScheduleId();
		}
		Integer integrationSchedule = unidirectionalInfo.getScheduleId();

		Integer scheduleIdToStore = getScheduleIdToStore(integrationSchedule, globalSchedule, isGraphQL);

		
		JobInstance jobInstance = saveJobInstance(instance, scheduleIdToStore, integrationInfo.getIntegrationName(),
				sourceEntity, sourceSystem);
		
		
		DeleteJobConfiguration deleteJobConfiguration = getDeleteJobConfiguration(unidirectionalInfo);
		
		// process max retry count if integration is updated
		String pollingTimeFromEvent = "";

		if (intgeration != null) {
			String maxRetryCountValue = unidirectionalInfo.getIntegrationContext().getMaxRetryCount();
			processUpdatedMaxCount(jobInstance.getJobInstanceId(), maxRetryCountValue);
			pollingTimeFromEvent = getMaxLastProcessedTime(jobInstance.getJobInstanceId());
		}
		deleteJobInstance = saveDeleteJobInstance(deleteJobInstance, integrationName, deleteJobConfiguration.getDeleteJobSchedule(), deleteJobConfiguration.isConfigureDeleteJob(), unidirectionalInfo);		
		EAIIntegrations integration = saveUnidirectionalIntegrationConfiguration(base, unidirectionalInfo, jobInstance,
				integrationName, sourceEntity, direction, deleteJobInstance);
		jobInstance.setOimIntegration(integration);

		//Merge the values from source context, criteria context and target context which are currently saved in job context. 
		Map<String, String> sourceContextMap = unidirectionalInfo.getSourceContext().toSourceContextMap();
		if (unidirectionalInfo.getTargetContext() != null)
			sourceContextMap.putAll(unidirectionalInfo.getTargetContext().fetchTargetContextSavedInJobContext());
		CriteriaContext criteria = unidirectionalInfo.getCriteria();
		if (criteria == null){
			criteria=new CriteriaContext();
		}
		sourceContextMap.putAll(criteria.toCriteriaContextMap());
		//Add default value of Regex in job context for TFS Git Commit Information
		if(SystemType.TFS.equals(sourceSystem.getSystemType().getSystemTypeId()) && TFSGitConstants.GIT_COMMIT_INFO.equals(unidirectionalInfo.getSourceContext().getIssueType())){
			String regexValue = sourceContextMap.get(TFSGitConstants.REGEX);
			if (StringUtils.isEmpty(regexValue)) {
				sourceContextMap.put(TFSGitConstants.REGEX, TFSGitConstants.DEFAULT_REGEX);
			}
		}

		// isGlobalScheduleConfigured to be considered as true only in case of
		// new ui/api call and if the integration level "As Per Global Setting"
		// option is selected
		boolean isGlobalScheduleConfigured = isGraphQL && JobSchedule.isGlobalSchedule(integrationSchedule);

		sourceContextMap.put(JobSchedule.IS_GLOBAL_SCHEDULE_CONFIGURED,String.valueOf(isGlobalScheduleConfigured));

		Map<String, String> targetContextMap = unidirectionalInfo.getTargetContext().toTargetContextMap();
		// Create Event time
		createOrUpdateEventTime(sourceSystem, jobInstance, sourceContextMap, groupInfo.getProjectMapping(), integration,
				criteria, unidirectionalInfo.getIntegrationContext().getPollingTime(),DateUtils.getUiFormatedDate(getPollingDate(jobInstance.getJobInstanceId())),
				unidirectionalInfo.getUnidirectionalId() != -1);


		Map<String, String> integrationContext = unidirectionalInfo.getIntegrationContext().toIntegrationContextMap();
		insertPollingTimeKeyForAudit(unidirectionalInfo.getIntegrationContext().getPollingTime(),integrationContext,integrationInfo.getIntegrationId().equals(-1), pollingTimeFromEvent);

		// Save integrationContext
		saveIntegrationContext(session, integration, integrationContext,targetContextMap,sourceEntity,groupInfo.getProjectMapping());
		// Create Source Context

		Map<String,String> jobContextMap=new HashMap<String,String>(sourceContextMap);
		jobContextMap.keySet().removeAll(Constants.SOURCE_PROCESS_CONTEXT_KEYS);
		
		createOrUpdateJobContext(session, jobInstance, sourceSystem, jobContextMap);
		if(deleteJobConfiguration.isConfigureDeleteJob()) {
			Map<String, String> deleteJobContext = new HashMap<>();
			String pollingDate = DeleteJobPollingType.POLLING_BY_DATE.equals(deleteJobConfiguration.getDeleteJobPollingType()) ? deleteJobConfiguration.getDeleteJobPollingDate(): null;
			deleteJobContext.put(Constants.DELETE_JOB_POLLING_DATE,pollingDate);
			createOrUpdateJobContext(session, deleteJobInstance, sourceSystem, deleteJobContext);
		}
		// Create ProcessContext
		if (unidirectionalInfo.getTargetLookUpContext() != null)
			targetContextMap.putAll(unidirectionalInfo.getTargetLookUpContext().toTargetLookupMap());

		createUpdateProcessContext(unidirectionalInfo.getEvents(), integration, sourceSystem.getSystemId(),destSystem.getSystemId(),sourceContextMap,targetContextMap,isGraphQL,unidirectionalInfo.getRemoteLinkContext());

		// Create Events Skipped List
		createUpdateEventsSkipped(unidirectionalInfo.getEvents(), integration);
		return integration;

	}

	/**
	 * @param session
	 * @param isCreate
	 * @param integration
	 * @return
	 * @throws EAIActionHandlerException
	 */
	private Integer getIntegrationModeForCreateOrUpdate(
			final EAIIntegrations integration, final boolean isCreate)
	{
		/*
		 * mode need to set when integration is created and set mode to
		 * integration mode. on update no change. if stored value null then set
		 * integration as mode
		 */
		return isCreate ? ConfigMode.INTEGRATION_MODE_ID : fetchIntegrationMode(integration).getModeId();


	}

	/**
	 * @param session
	 * @param eaiIntegrations
	 * @return
	 * @throws EAIActionHandlerException
	 */
	private ConfigMode fetchIntegrationMode(final EAIIntegrations eaiIntegrations)
	{

		Integer modeValue = eaiIntegrations != null ? eaiIntegrations.getConfigMode() : null;
		if (modeValue != null && modeValue.equals(ConfigMode.RECONCILE_MODE_ID)) {
			return ConfigMode.RECONCILE;

		}
		return ConfigMode.INTEGRATION;

	}



	private void createUpdateEventsSkipped(final List<EAISourceEventInfo> events, final EAIIntegrations integration) {
		Iterator<EAISourceEventInfo> eventsItr = events.iterator();
		List<String> skippedEvents = new ArrayList<>();
		while (eventsItr.hasNext()) {
			EAISourceEventInfo sourceEvents = eventsItr.next();
			if (sourceEvents.isSkipped()) {
				skippedEvents.add(sourceEvents.getEventName());
			}
		}

		Map<String, EAIEventsSkipped> existingSkippedEvents = getExistingSkippedEvents(integration.getIntegrationId());
		saveSkippedEvents(integration, skippedEvents, existingSkippedEvents);
	}

	/**
	 * Updates(Add new entries / Remove existing entries) the entries in database of event types which are to be skipped
	 */
	private void saveSkippedEvents(final EAIIntegrations integration, final List<String> skippedEvents,final Map<String,EAIEventsSkipped> existingSkippedEvents) {
		for (String eventName : skippedEvents) {
			// No Processing is required
			if (existingSkippedEvents.get(eventName) != null)
				existingSkippedEvents.remove(eventName);
			// Add event type id in events skipped table
			else {
				EAIEventsSkipped eventSkipped = new EAIEventsSkipped();
				eventSkipped.setIntegration(integration);
				eventSkipped.setEventType(EaiEventType.getEventType(session, eventName));
				session.saveOrUpdate(eventSkipped);
			}
		}

		// Delete remaining entries
		Iterator<Entry<String, EAIEventsSkipped>> existingEvents = existingSkippedEvents.entrySet().iterator();
		while (existingEvents.hasNext()) {
			Entry<String, EAIEventsSkipped> event = existingEvents.next();
			session.delete(event.getValue());
		}
	}

	/**
	 * Returns Map with key as event type name , of existing event types which are skipped per integration 
	 */
	private Map<String, EAIEventsSkipped> getExistingSkippedEvents(final Integer integrationId) {
		Map<String, EAIEventsSkipped> existingSkippedEvents = new HashMap<>();
		Query mappingQry = session.createQuery(FROM + EAIEventsSkipped.class.getName() + WHERE_INTEGRATION_ID);
		mappingQry.setParameter("id", integrationId);
		List<EAIEventsSkipped> events = mappingQry.getResultList();
		for (EAIEventsSkipped skippedEvents : events) {
			existingSkippedEvents.put(skippedEvents.getEventType().getEventType(), skippedEvents);
		}
		return existingSkippedEvents;
	}

	private String getOldIntegrationName(final Integer integrationId) throws ORMException, LoggerManagerException {
			LoggerManager objLoggerManager = new LoggerManager();
			return objLoggerManager.getIntegrationNameFromId(integrationId);
	}

	private EAIIntegrations saveUnidirectionalIntegrationConfiguration(final EAIIntegrationsBase base,
			final UnidirectionalIntegrationData unidirectionalInfo, final JobInstance jobInstance,
			final String integrationName, final EntityMeta sourceEntity, final Integer direction, final JobInstance deleteJobInstance) {
		EAIIntegrations integration;
		boolean isCreate = false;

		if (unidirectionalInfo.getUnidirectionalId() != null && unidirectionalInfo.getUnidirectionalId() > 0) {
			integration = session.get(EAIIntegrations.class, unidirectionalInfo.getUnidirectionalId());

		} else {
			integration = new EAIIntegrations();
			isCreate = true;
		}

		integration.setBaseIntegration(base);
		integration.setIntegrationName(integrationName);
		integration.setSourceJobInstance(jobInstance);
		EAIIntegrationStatus integrationStatus = session.get(EAIIntegrationStatus.class, EAIIntegrationStatus.INACTIVE);
		integration.setStatus(integrationStatus);
		// set polling type of integration configured
		EAIPollingType pollingType = session.get(EAIPollingType.class, unidirectionalInfo.getPollingType());
		integration.setPollingType(pollingType);
		integration.setEntityDisplayName(sourceEntity.getEntityDisplayName());
		integration.setLastUpdatedOn(Calendar.getInstance().getTime());
		integration.setDirection(direction);
		integration.setConfigMode(getIntegrationModeForCreateOrUpdate(integration, isCreate));
		integration.setDeleteJobInstance(deleteJobInstance);

		session.saveOrUpdate(integration);

		return integration;
	}

	private void createOrUpdateEventTime(final Systems sourceSystem, final JobInstance jobInstance,final Map<String, String> sourceContextMap,final List<ProjectMappingContext> projectMapping,
			final EAIIntegrations integration, final CriteriaContext criteria, final String generationTime,
			final String existingPollingTime, final boolean isEdit) throws OpsHubBaseException {

		HasMetadata metadataImplClass = MetadataImplFactory.getMetadataImplClass(session, sourceSystem);

		// if child project polling supported then add child project mapping
		// also so that further event time and project mapping will be preserved
		// and if any child project event is in recovery state then it will be
		// preserved
		if (metadataImplClass.isChildProjectPollingSupported()) {
			List<EAIProjectMappings> existingChildProjMappings = integration.getBaseIntegration().getIntegrationGroup()
					.getProjectMappingRefBase().getEaiProjectMappings().stream()
					.filter(proj -> proj.isChildProjAutoMapped()).collect(Collectors.toList());

			List<ProjectMappingContext> existingChildProjMappingContexts = IntegrationObjectformatConverter
					.convertEAIProjectMappingToProjectMappingContext(existingChildProjMappings);

			Map<String, String> allProjects = EaiUtility.convertProjectMetaToMap(metadataImplClass.getProjectsMeta());
			//From existing child projects, only those projects should be continued for which parent is stil maped.
			processExistingAutoMappedProjects(projectMapping, integration, existingChildProjMappingContexts,
					allProjects);
		}

		EaiUtility.createUpdateEventTime(session, sourceSystem, jobInstance, generationTime,existingPollingTime, isEdit,
				sourceContextMap.get(EAIJobConstants.AccuWork.START_TRANS_NUM), sourceContextMap,projectMapping, integration,
				criteria, metadataImplClass);
	}

	private void processExistingAutoMappedProjects(final List<ProjectMappingContext> projectMapping,
			final EAIIntegrations integration, final List<ProjectMappingContext> existingChildProjMappingContexts,
			final Map<String, String> allProjects) {
		Set<String> parentPaths = new HashSet<>();
		//Construct all possible parent path(s) from user-mapped project.
		for (ProjectMappingContext projectMappingContext : projectMapping) {
			String prjID = integration.getDirection() == ConfigDirection.FORWARD_ID
					? projectMappingContext.getEp1Project() : projectMappingContext.getEp2Project();
			if (allProjects.containsKey(prjID)) {
				parentPaths.add(allProjects.get(prjID));
			}
		}
		prepareProjectMapping(projectMapping, integration, existingChildProjMappingContexts, allProjects, parentPaths);
	}

	
	private void prepareProjectMapping(final List<ProjectMappingContext> projectMapping,
			final EAIIntegrations integration, final List<ProjectMappingContext> existingChildProjMappingContexts,
			final Map<String, String> allProjects, final Set<String> parentPaths) {
		//Iterate over existing project mappings from db.
		for (ProjectMappingContext projectMappingContext : existingChildProjMappingContexts) {
			String prjID = integration.getDirection() == ConfigDirection.FORWARD_ID
					? projectMappingContext.getEp1Project() : projectMappingContext.getEp2Project();
			if (allProjects.containsKey(prjID)) {
				String prjPath = allProjects.get(prjID);
				//Only continue with those child projects, whose parent projects are mapped.
				for (String parentPath : parentPaths) {
					if (prjPath.startsWith(parentPath + "/") && !projectMapping.contains(projectMappingContext)) {
						projectMapping.add(projectMappingContext);
						break;
					}
				}
			}
		}
	}

	private JobInstance saveJobInstance(JobInstance instance, final Integer scheduleId, final String integrationName,
			final EntityMeta sourceEntity, final Systems sourceSystem) 
					throws OpsHubBaseException {

		if (instance != null) {
			instance.setJobName(
					integrationName + " " + sourceSystem.getDisplayName() + " " + sourceEntity.getEntityDisplayName());
			// if integration's job is active then do not allow editing.
			session.refresh(instance);
			if (instance.getStatus().getId().equals(EAIIntegrationStatus.ACTIVE))
				throw new OpsHubBaseException("012022", null, null);
			else {
				JobSchedule associateSchedule = session.get(JobSchedule.class, scheduleId);
				instance.setSchedule(associateSchedule);
			}
			session.saveOrUpdate(instance);
		} else {
			instance = createJobInstance(session, integrationName, sourceSystem.getSystemId(), sourceEntity,
					scheduleId);
		}
		return instance;
	}

	/**
	 * 
	 * @param scheduleId
	 * @param globalScheduleId
	 * @return schedule id for the integration,
	 * @throws DataValidationException
	 */
	private Integer getScheduleIdToStore(final Integer scheduleId, final String globalScheduleId,
			final boolean isGraphQL) throws DataValidationException {

		Integer scheduleToStore = scheduleId;
		// Global Schedule, consider the value according to global parameter
		if (JobSchedule.isGlobalSchedule(scheduleId)) {
			scheduleToStore = getGlobalScheduleToStore(globalScheduleId, isGraphQL);
		} else if (scheduleId.intValue() < 0) {

			String scheduleValue = null;
			if (scheduleId != null) {
				scheduleValue = scheduleId.toString();
			}
			throw new DataValidationException("017661", new String[] { scheduleValue }, null);
		}
		return scheduleToStore;
	}

	private Integer getGlobalScheduleToStore(final String globalScheduleId, final boolean isGraphQL)
			throws DataValidationException {

		int scheduleToStore;
	
		if (org.apache.commons.lang3.StringUtils.isNumeric(globalScheduleId)) {
			int globalScheduleIdNumeric = Integer.parseInt(globalScheduleId);
			//Schedule id needs to be a positive integer 
			if (globalScheduleIdNumeric < 0) {
				throw new DataValidationException("017661", new String[] { String.valueOf(globalScheduleIdNumeric) }, null);
			} else {
				scheduleToStore = globalScheduleIdNumeric;
			}
		} else {
			if (isGraphQL) {// new ui, invalid input case
				throw new DataValidationException("017660", new String[] { globalScheduleId }, null);
			} else {
				scheduleToStore = JobSchedule.SIMPLE_4;// old
																						// ui
																						// case
			}
		}
		return scheduleToStore;
	}


	private EntityMeta getSourceEntityMeta(final Systems sourceSystem, final String sourceIssueType, final List<ProjectMappingContext> projectMapping, final Integer direction)
			throws MetadataImplFactoryException, MetadataException {
		HasMetadata metadataImplClass = MetadataImplFactory.getMetadataImplClass(session, sourceSystem);
		Set<String> projects = new HashSet<>();
		if (!projectMapping.isEmpty()) {
			for (ProjectMappingContext projectMappingContext : projectMapping) {
				if(ConfigDirection.FORWARD_ID.equals(direction) && !ConfigDirection.BACKWARD.equals(projectMappingContext.getDirection()))
					projects.add(projectMappingContext.getEp1Project());
				else if(ConfigDirection.BACKWARD_ID.equals(direction) && !ConfigDirection.FORWARD.equals(projectMappingContext.getDirection()))
					projects.add(projectMappingContext.getEp2Project());
			}
		}
		return metadataImplClass.getEntityMeta(sourceIssueType, Joiner.on(',').join(projects));
	}

	private EAIIntegrationsBase createUpdateIntegrationBase(final BidirectionalIntegrationInfo integrationInfo,final EAIIntegrationGroup integrationGroup) {
		EAIIntegrationsBase integrationBase;
		if (integrationInfo.getIntegrationId() == -1) {
			integrationBase = new EAIIntegrationsBase();
		} else {
			integrationBase = loadIntegrationBase(integrationInfo.getIntegrationId());
		}
		integrationBase.setIntegrationName(integrationInfo.getIntegrationName());
		integrationBase.setDirection(integrationInfo.getDirection().getDirectionId());
		
		integrationBase.setIntegrationGroup(integrationGroup);
		session.saveOrUpdate(integrationBase);
		return integrationBase;
	}

	private void processUpdatedMaxCount(final Integer jobInstanceId, final String maxRetryCountValue) {
		// Set notified to false if max retry count is increased in integration
		// form and events are notified already.
		Integer newMaxRetryCount;
		if (maxRetryCountValue == null || maxRetryCountValue.isEmpty()) {
			newMaxRetryCount = 0;
		} else {
			newMaxRetryCount = Integer.valueOf(maxRetryCountValue);
		}
		HashMap<String, String> jobCtx = EaiUtility.getJobContextValues(session, jobInstanceId);
		String oldRetryCount = jobCtx.get(EAIJobConstants.MAXRETRYCOUNT);
		int oldMaxRetryCount = EAIJobConstants.MAXRETRYCOUNT_VAL;
		if (StringUtils.isNotEmpty(oldRetryCount)) {
			oldMaxRetryCount = Integer.valueOf(oldRetryCount);
		}
		if (newMaxRetryCount > oldMaxRetryCount) {
			String failedNotifiedEventsQuery = FROM + EaiEventsLogged.class.getName()
					+ " where sourceJobInstance.jobInstanceId=" + jobInstanceId + " and notified=1";
			Query failedNotifiedEvents = session.createQuery(failedNotifiedEventsQuery);
			List<EaiEventsLogged> failedNotifiedLog = failedNotifiedEvents.list();
			for (EaiEventsLogged log : failedNotifiedLog) {
				log.setNotified(false);
				session.saveOrUpdate(log);
			}
		}

	}

	private EAIIntegrationsBase loadIntegrationBase(final Integer id) {
		return session.get(EAIIntegrationsBase.class, id);
	}

	public EAIIntegrations loadIntegrationFromBase(final Integer id) {
		Query query = session.createQuery(FROM + EAIIntegrations.class.getName() + WHERE_BASE_INTEGRATION_INTEGRATION_BASE_ID + id);

		List<EAIIntegrations> objIntegrations = query.list();
		if (objIntegrations != null && !objIntegrations.isEmpty()) {
			return objIntegrations.get(0);
		}
		return null;
	}

	/**
	 * Given integration group id, returns the first bidirectional integration in the group
	 */
	public EAIIntegrationsBase loadBaseFromGroup(final Integer groupId) {
		Query query = session.createQuery(FROM + EAIIntegrationsBase.class.getName() + " where integrationGroup.groupId=" + groupId);

		List<EAIIntegrationsBase> objIntegrations = query.list();
		if (objIntegrations != null && !objIntegrations.isEmpty()) {
			return objIntegrations.get(0);
		}
		return null;
	}

	private void validateConfiguration(final Systems ep1System, final Systems ep2System,
			final BidirectionalIntegrationInfo integrationInfo) throws OpsHubBaseException {
		OpsHubLoggingUtil.trace(LOGGER, "Validating integration configuration", null);
		OIMConfiguration ep1Configuration = CustomConfigurationFactory
				.getOIMConfiguration(ep1System.getSystemType().getSystemTypeId());

		OIMConfiguration ep2Configuration = CustomConfigurationFactory
				.getOIMConfiguration(ep2System.getSystemType().getSystemTypeId());

		if (integrationInfo.isForwardIntegrationConfigured()) {
			validateOIMConfiguration(ep1Configuration, integrationInfo.getForwardSettings(),
					integrationInfo.getIntegrationId() != -1, true);
			validateOIMConfiguration(ep2Configuration, integrationInfo.getBackwardSettings(),
					integrationInfo.getIntegrationId() != -1, false);
		}

		if (integrationInfo.isBackwardIntegrationConfigured()) {
			validateOIMConfiguration(ep1Configuration, integrationInfo.getForwardSettings(),
					integrationInfo.getIntegrationId() != -1, false);
			validateOIMConfiguration(ep2Configuration, integrationInfo.getBackwardSettings(),
					integrationInfo.getIntegrationId() != -1, true);
		}

	}

	private void validateOIMConfiguration(final OIMConfiguration configuration,
			final UnidirectionalIntegrationData info, final boolean edit, final boolean isSource)
			throws OpsHubBaseException {
		if (configuration != null) {
			configuration.validateOIMConfiguration(session, info, edit, isSource);
		}
	}

	/**
	 * This method verified the license and edition features
	 * for all the bidirectional integrations in the parameters.
	 * @param bidirectional integration list for which the validation needs to be made.
	 */
	public void verifyLicenseAndEditionFeatures(final List<BidirectionalIntegrationInfo> integrationInfoList)
			throws OpsHubBaseException {
		OpsHubLoggingUtil.trace(LOGGER, "Verifying license and edition features", null);
		List<IntegrationTargetPointInfo> itpiList = new ArrayList<>();
		for(BidirectionalIntegrationInfo integrationInfo: integrationInfoList) {
			Map<String, String> issueTypeMap = getIssueTypes(integrationInfo);
			
			IntegrationTargetPointInfo itpi = new IntegrationTargetPointInfo(integrationInfo.getIntegrationName(),
					integrationInfo.getEndpoint1(), integrationInfo.getEndpoint2(), issueTypeMap.get(ENDPOINT_1_ISSUETYPE),
					issueTypeMap.get(ENDPOINT_2_ISSUETYPE), integrationInfo.getIntegrationId().equals(-1),
					integrationInfo.getGroupInfo().getProjectMapping(), integrationInfo.getIntegrationId());
			
			itpiList.add(itpi);
		}
		
		LicenseVerifier licVerifier = new LicenseVerifier(session, itpiList);
		licVerifier.verify();

		LicenseEditionVerifier licEditionVerifier = new LicenseEditionVerifier(session);

		// Verify EditionFeatures for all the integrations before creating new or
		// updating integration
		for(BidirectionalIntegrationInfo integrationInfo: integrationInfoList) {
			LicenseEndpointVerificationData verificationData = new LicenseEndpointVerificationData();
			verificationData.setIntegrationId(integrationInfo.getIntegrationId());
			verificationData.addInvolvedEndpointIds(integrationInfo.getEndpoint1());
			verificationData.addInvolvedEndpointIds(integrationInfo.getEndpoint2());
			licEditionVerifier.verifyEditionFeatures(verificationData);	
		}
	}

	/**
	 * This method is used to get issuetypes configured in integration from
	 * forward or backward settings, whichever is configured
	 * 
	 * @param integrationInfo
	 * @return
	 */
	private Map<String, String> getIssueTypes(final BidirectionalIntegrationInfo integrationInfo) {
		Map<String, String> issueTypeMap = new HashMap<>();
		if (integrationInfo.isForwardIntegrationConfigured()) {
			UnidirectionalIntegrationData forwardSettings = integrationInfo.getForwardSettings();
			issueTypeMap.put(ENDPOINT_1_ISSUETYPE, forwardSettings.getSourceContext().getIssueType());
			issueTypeMap.put(ENDPOINT_2_ISSUETYPE, forwardSettings.getTargetContext().getIssueType());
		} else if (integrationInfo.isBackwardIntegrationConfigured()) {
			UnidirectionalIntegrationData backwardSettings = integrationInfo.getBackwardSettings();
			issueTypeMap.put(ENDPOINT_2_ISSUETYPE, backwardSettings.getSourceContext().getIssueType());
			issueTypeMap.put(ENDPOINT_1_ISSUETYPE, backwardSettings.getTargetContext().getIssueType());
		}
		return issueTypeMap;
	}

	/**
	 * This will return list of integration Ids from given integration base ids
	 * and given config mode.
	 * 
	 * @param groupIds
	 * @return
	 */
	public List<Integer> getIntegrationIds(final List<Integer> integrationBaseIds, final Integer configMode) {

		// get integration base ids split list
		// this is needed when integration base id list is more than 100 at that
		// time
		// oracle db can not allow >100 elements in clause.
		List<List<Integer>> intBaseIdsSplitList = DbOperationsUtil.INSTANCE.splitIdsInChunks(integrationBaseIds);
		List integrationIds = new ArrayList<>();

		// iterate over integration base id chunk to get integration id list
		for (List<Integer> integrationBaseIdChunk : intBaseIdsSplitList) {

			// create query on EAIIntegrations
			CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
			CriteriaQuery<EAIIntegrations> query = criteriaBuilder.createQuery(EAIIntegrations.class);
			Root<EAIIntegrations> fromIntegrations = query.from(EAIIntegrations.class);

			// config mode clause
			Predicate configModePredicate = criteriaBuilder.equal(fromIntegrations.get("configMode"), configMode);

			// join to integration base table
			Join<EAIIntegrations, EAIIntegrationsBase> intConfJoinIntBaseJoinIntGrp = fromIntegrations
					.join("baseIntegration", JoinType.INNER);
			// integration ids where clause
			Expression<Integer> integrationIdExp = intConfJoinIntBaseJoinIntGrp.get("integrationBaseId");
			Predicate finalWhereClause = criteriaBuilder.and(configModePredicate,
					integrationIdExp.in(integrationBaseIdChunk));

			// final where clause
			query.where(finalWhereClause);

			// select only integration ids
			query.select(fromIntegrations.get("integrationId"));

			integrationIds.addAll(session.createQuery(query).getResultList());
		}

		return integrationIds;
	}

	public List<String> changeBidirectionalStatus(final List<Integer> integrationBaseIds, final String status)
			throws OpsHubBaseException, SchedulerException {

		// get integration id list where integration is configured
		List<Integer> integrationIdsLst = getIntegrationIds(integrationBaseIds,
				ConfigMode.INTEGRATION_MODE_ID);

		// get integration ids list where reconcile is configured
		List<Integer> reconcileIntegrationIdsAllLst = getIntegrationIds(integrationBaseIds,
				ConfigMode.RECONCILE_MODE_ID);

		// List of reconcile integration id where action is needed
		List<String> reconcileIntegrationIdsActionList = new ArrayList<>();

		List<GroupMessageInfo> canNotChangeStatusRecoInfo = new ArrayList<>();
		List<GroupMessageInfo> canNotExecuteStatusForReco = new ArrayList<>();
		for (Integer reconcileIntegrationId : reconcileIntegrationIdsAllLst) {
			/*
			 * If reconcile mode then expected to change reconcile state
			 * provided its current state is not expired. as after
			 * Completed/expired reconcile need to reconfigure prior to change
			 * the state
			 */
			EAIReconciliations reconcile = getReconciliationInfoFromIntegration(session, reconcileIntegrationId);

			GroupIntegrationInfo groupIntegrationInfo = new GroupIntegrationInfo(
					reconcile.getEaiIntegrations().getBaseIntegration().getIntegrationGroup().getGroupName(),
					reconcile.getReconciliationName(),
					reconcile.getEaiIntegrations().getBaseIntegration().getIntegrationGroup().getGroupId(),
					reconcile.getReconciliationId(),
					reconcile.getEaiIntegrations().getBaseIntegration().getIntegrationBaseId());
			if (!EXECUTE.equals(status)) {
				if (!reconcile.getReconcileJobInstance().getStatus().getId().equals(JobStatus.EXPIRED)) {
					handleReconciliationBidirectionalUpdate(reconcile, canNotChangeStatusRecoInfo,
							reconcileIntegrationIdsActionList, groupIntegrationInfo, status);
				} else {
					canNotChangeStatusRecoInfo.add(new GroupMessageInfo(groupIntegrationInfo,
							ReconcileValidation.ERROR_MSG_FOR_GROUP_STATUS_CHANGE_WITH_EXPIRED_RECO));
				}
			} else {
				canNotExecuteStatusForReco.add(
						new GroupMessageInfo(groupIntegrationInfo, ReconcileValidation.EXECUTE_STATUS_NOT_ALLOWED));
			}
		}

		List<String> response = changeStatus(session, integrationIdsLst, status, true);
		//Setting suppress validation warning to true, because we don't want reco to display any error when migration license is not present.
		response.addAll(changeReconcileStatus(session, reconcileIntegrationIdsActionList, status, true, true));
		GroupMessageInfo.addGroupMessageString(response, canNotChangeStatusRecoInfo, ReconcileValidation.RECONCILE,
				GroupMessageInfo.ERROR);
		GroupMessageInfo.addGroupMessageString(response, canNotExecuteStatusForReco, ReconcileValidation.RECONCILE,
				GroupMessageInfo.ERROR);
		return response;

	}


	private void handleReconciliationBidirectionalUpdate(final EAIReconciliations reconcile,
			final List<GroupMessageInfo> canNotChangeStatusRecoInfo,
			final List<String> reconcileIntegrationIdsActionList, final GroupIntegrationInfo groupIntegrationInfo,
			final String status)
			throws LicenseException {
		ReconcilationWithMigrationLicenseVerifier recoMigLicVerifier = new ReconcilationWithMigrationLicenseVerifier(
				session);
		if (JobStatus.INACTIVE.equals(status) || JobStatus.PAUSE_STATUS.equals(status)) {
			reconcileIntegrationIdsActionList.add(reconcile.getReconciliationId().toString());
			return;
		}
		if (!recoMigLicVerifier.isReconcilationDateRequireChange(reconcile.getEaiIntegrations(),
				reconcile.getReconcileJobInstance())) {
			reconcileIntegrationIdsActionList.add(reconcile.getReconciliationId().toString());
		} else {
			canNotChangeStatusRecoInfo.add(new GroupMessageInfo(groupIntegrationInfo,
					ReconcileValidation.RECONCILE_ACTIVE_STATUS_NOT_ALLOWED_WITHOUT_MIGRATION_LIC));
		}
	}


	public List<Integer> getConfIdsFromBase(final Integer baseId) {
		List<Integer> integrationIds = new ArrayList<>();
		Query query = session.createQuery("select integrationId " + FROM + EAIIntegrations.class.getName()	+ WHERE_BASE_INTEGRATION_INTEGRATION_BASE_ID + baseId);

		List<Integer> objIntegrations = query.list();
		if (objIntegrations != null && !objIntegrations.isEmpty()) {
			integrationIds.addAll(objIntegrations);
		}
		return integrationIds;
	}

	/**
	 * Deletes bidirectional integration if all integrations associated with it are in INACTIVE state.
	 * @param baseId : bidirectional integration id
	 * @return : empty String if deletion is successful, else returns the group name
	 */
	public String deleteBidirectionalIntegration(final EAIIntegrationsBase base, final boolean deleteGroup) throws OpsHubBaseException {
		int baseId = base.getIntegrationBaseId();
		List<Integer> ids = getConfIdsFromBase(baseId);
		boolean canDelete = true;
		String groupName = "";
		for (int integrationCounter = 0; integrationCounter < ids.size(); integrationCounter++) {
			Integer integrationId = Integer.valueOf(ids.get(integrationCounter).toString());
			int status = getStatus(session, integrationId);
			canDelete = canDelete && !EAIIntegrationStatus.ACTIVE.equals(status);
		}

		//Delete only if both the unidirectional integrations are in INACTIVE state.
		if (canDelete) {
			for (int integrationCounter = 0; integrationCounter < ids.size(); integrationCounter++) {
				Integer integrationId = Integer.valueOf(ids.get(integrationCounter).toString());
				deleteIntegration(integrationId, deleteGroup);
			}
		} else {
			groupName = base.getIntegrationGroup().getGroupName();
		}
		return groupName;
	}

	/**
	 * 
	 * 
	 * 
	 * @param session
	 * @param user
	 * @param isNameUpdated
	 * @param forwardSettings
	 * @throws LoggerManagerException
	 * @throws ORMException
	 * @throws IOException
	 * @throws SchedulerException
	 * @throws OpsHubBaseException
	 * 
	 *             check reconcile. if reconcile configured then update target
	 *             look up context of reconcile name from integration context
	 * 
	 */
	private void updateRecoSettingsFromIntegration(final Session session, final boolean isNameUpdated,
			final String oldIntegrationName, final EAIIntegrations integration, final TargetLookup targetLookup,
			final boolean isGraphQL) throws ORMException, LoggerManagerException, IOException {
		if (isGraphQL) {
			// TODO once old ui deprecated then remove graph ql boolean and
			// always update or remove this method if we remove these property
			// from reco table

			EAIReconciliations eaiReconcile = getReconciliationInfoFromId(session, integration.getIntegrationId());
			if (eaiReconcile != null) {
				// if reco for integration exist
				eaiReconcile.setReconciliationName(integration.getIntegrationName());
				if (targetLookup != null && targetLookup.getTargetLookUpQuery() != null && !StringUtils.isEmpty(targetLookup.getTargetLookUpQuery())) {
					eaiReconcile.setTargetSearchQuery(targetLookup.getTargetLookUpQuery());
					String isContinueToUpdate = targetLookup.getIsContinueUpdate();
					eaiReconcile.setTargetQueryCriteria(
							isContinueToUpdate != null ? Integer.parseInt(isContinueToUpdate)
									: Constants.FAIL_IF_MULTIPLE_FOUND);
				}
				session.saveOrUpdate(eaiReconcile);

				if (isNameUpdated) {
					changeLogFileName(eaiReconcile.getReconciliationId(), oldIntegrationName,
							LoggerManager.RECONCILIATION);
				}
			}
		}
	}

	public void updateJobInstanceForConcurrentReadJob(final int jobInstnaceId) {
		JobInstance jobInstance = session.get(JobInstance.class, jobInstnaceId);
		Job job = session.get(Job.class, PARALLEL_PROCESSING_JOB_ID);
		jobInstance.setJob(job);
		session.saveOrUpdate(jobInstance);
	}

	/**
	 * method return the max and min last processed event time of group
	 * 
	 * 
	 * @param session
	 * @param groupId
	 * @param direction
	 * @param funcation
	 * @return
	 */
	public List<EAIKeyValue> getGroupEventProcessedTime(final Session session, final Integer groupId,
			final Integer direction) {
		final String DIRECTION = "DIRECTION";
		final String GROUP_ID = "groupId";

		String queryStr = "select new map(max(lastProcessedTime) as maxLastProcessedTime, min(lastProcessedTime) "
				+ "as minLastProcessedTime) from " + EAIGroupSyncProgress.class.getName()
				+ " where integrationDirection=:" + DIRECTION + " and groupId=:" + GROUP_ID
				+ " and lastProcessedTime <> " + IntegrationConfiguration.MINUS_ONE + " group by groupId";
		
		org.hibernate.query.Query query = session.createQuery(queryStr);
		query.setParameter(DIRECTION, String.valueOf(direction));
		query.setParameter(GROUP_ID, groupId);
		List<Map> maxLatProcessedEvent = query.getResultList();

		List<EAIKeyValue> eaiKeyValues = new ArrayList<>();
		if(!EaiUtility.isNullOrEmptyList(maxLatProcessedEvent)){
			eaiKeyValues.add(
					new EAIKeyValue(Constants.MAX_FUNCATION,maxLatProcessedEvent.get(0).get("maxLastProcessedTime")==null?null:String.valueOf(maxLatProcessedEvent.get(0).get("maxLastProcessedTime"))));
			eaiKeyValues.add(
					new EAIKeyValue(Constants.MIN_FUNCATION,maxLatProcessedEvent.get(0).get("minLastProcessedTime")==null?null:String.valueOf(maxLatProcessedEvent.get(0).get("minLastProcessedTime"))));

		}
		return eaiKeyValues;

		}


	public List<EventTime> getReconcileEventTimes(final Session session, final Integer reconciliationId) {
		ReconcileDBHelper reconcileDBHelper = new ReconcileDBHelper();
		return reconcileDBHelper.getReconcileEventTimes(session, reconciliationId);

	}

	private void setCriteriaProcessedDateInJobContext(final Session session, final Date criteriaTillDate,
			final JobInstance jobInstance) {
		//Sets date in job context of reconciliation
		if (criteriaTillDate != null) {
			JobContext jobContext = new JobContext(jobInstance, "criteria_processed_till_date",
					String.valueOf(criteriaTillDate.getTime()));
			session.saveOrUpdate(jobContext);

		}
	}

	/**
	 * Accepts list of integration ids and filters out the integrations that are
	 * not currently syncing any data. It only returns ids of integrations that
	 * are syning data.
	 * 
	 * @param session
	 * @param integrationIds
	 * @param mergeIntegrationRunningStatus
	 * @return
	 * @throws OpsInternalError
	 */
	public List<Integer> filterCurrentlyExecutingIntegrations(final Session session,
			final List<EAIIntegrationStatus> integrations, final boolean mergeIntegrationRunningStatus)
			throws OpsInternalError {
		List<Integer> executingIntegrations = new ArrayList<Integer>();
		// Getting currently executing jobs
		List jobs = getSchedulerJobs();
		for (EAIIntegrationStatus integration : integrations) {
			// First checking if integration is active, then checking if a job
			// is active for given integration. If it is add it to executing
			// list.
			if (integration.getStatusName().equals(EAIIntegrationStatus.ACTIVE_STATUS_NAME)
					&& isIntegrationJobRunning(session, integration.getId(), mergeIntegrationRunningStatus, jobs)) {
				executingIntegrations.add(integration.getId());
			}
		}
		return executingIntegrations;
	}

	/*
	 * Accepts map of integration ids and status and filters out the
	 * integrations that are not currently syncing any data. It only returns ids
	 * of integrations that are syning data. Here, the status is mapped with the
	 * backend values instead of api values
	 */
	public List<Integer> filterCurrentlyExecutingIntegrations(final Session session,
			final Map<Integer, EAIIntegrationStatus> integrationsWithStatus, final boolean mergeIntegrationRunningStatus)
			throws OpsInternalError {
		List<Integer> executingIntegrations = new ArrayList<Integer>();
		// Getting currently executing jobs
		List jobs = getSchedulerJobs();
		for (Integer integrationId : integrationsWithStatus.keySet()) {
			// First checking if integration is active, then checking if a job
			// is active for given integration. If it is add it to executing
			// list.
			if (EAIIntegrationStatus.ACTIVE.equals(integrationsWithStatus.get(integrationId).getId())
					&& isIntegrationJobRunning(session, integrationId, mergeIntegrationRunningStatus, jobs)) {
				executingIntegrations.add(integrationId);
			}
		}
		return executingIntegrations;
	}

	public boolean isIntegrationJobRunning(final Session session, final int integrationId,
			final boolean mergeIntegrationRunningStatus) throws OpsInternalError {
		List jobs = getSchedulerJobs();
		return isIntegrationJobRunning(session, integrationId, mergeIntegrationRunningStatus, jobs);
	}

	/**
	 * Accepts integration id and returns if that integration is currently in
	 * execution or not. i.e. if it is syncing any data or not.
	 * 
	 * @param session
	 * @param integrationId
	 * @param mergeIntegrationRunningStatus
	 * @param jobs
	 * @return
	 * @throws OpsInternalError
	 */
	public boolean isIntegrationJobRunning(final Session session, final int integrationId,
			final boolean mergeIntegrationRunningStatus, final List jobs)
			throws OpsInternalError {
		// Getting job id for current integration id.
		EAIIntegrations eaiIntegration = session.get(EAIIntegrations.class, integrationId);
		boolean isJobRunning = isJobRunning(session, eaiIntegration.getSourceJobInstance(), mergeIntegrationRunningStatus, jobs);
		
		//check if delete job is running
		if(eaiIntegration.getDeleteJobInstance()!=null) {
			isJobRunning = isJobRunning || isJobRunning(session, eaiIntegration.getDeleteJobInstance(), mergeIntegrationRunningStatus, jobs);
		}
		
		return isJobRunning;
	}
	
	private boolean	isJobRunning(final Session session, final JobInstance jobInstance,
			final boolean mergeIntegrationRunningStatus, final List jobs) {		
		String argJobId = String.valueOf(jobInstance.getJobInstanceId());
		// Checking if the job is currently in execution or not.
		boolean isJobRunning = isIntegrationInExecution(argJobId, jobs);
		// In automation, sometimes we want to return true even when job is not
		// currently in execution but its integration is active. So in the
		// condition below, we check if we want to merge execution status and
		// integration status or not. If the condition is evaluated as true,
		// then we will return true if integration is active (even if no data is
		// being synchronized by that integration.)
		if (mergeIntegrationRunningStatus) {
			isJobRunning = isIntegrationJobActive(isJobRunning, session, jobInstance);
		}
		return isJobRunning;
	}

	/**
	 * Accepts job id and list of jobs and checks if the id is present in the
	 * list or not. If it is present that means that the integration is
	 * currently in execution.
	 * 
	 * @param argJobId
	 * @param jobs
	 * @return
	 */
	private boolean isIntegrationInExecution(final String argJobId, final List jobs) {
		if (jobs != null) {
			for (Object job : jobs) {
				if (job instanceof JobExecutionContext) {
					// this contains job id details in name attribute
					String jobName = ((JobExecutionContext) job).getJobDetail().getName();
					if (argJobId.equalsIgnoreCase(jobName)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * Get currently active jobs.
	 * 
	 * @return
	 * @throws OpsInternalError
	 * @throws SchedulerException
	 */
	private List getSchedulerJobs() throws OpsInternalError {
		Scheduler sched;
		List jobs = new ArrayList<>();
		SchedulerFactory schedFact = new StdSchedulerFactory();
		try {
			sched = schedFact.getScheduler();
			jobs = sched.getCurrentlyExecutingJobs();
		} catch (SchedulerException e) {
			throw new OpsInternalError("009001", new String[] { e.getLocalizedMessage() }, e);
		}
		return jobs;
	}

	/**
	 * In automation, sometimes we want to return true even when job is not
	 * currently in execution but its integration is active. So here, then we
	 * will return true if integration is active (even if no data is being
	 * synchronized by that integration.)
	 * 
	 * @param isJobRunning
	 * @param session
	 * @param jobInstance
	 * @return
	 */
	private boolean isIntegrationJobActive(boolean isJobRunning, final Session session, final JobInstance jobInstance) {
		if (!isJobRunning) {
			session.refresh(jobInstance);
			if (JobStatus.ACTIVE.equals(jobInstance.getStatus().getId())) {
				isJobRunning = true;
				OpsHubLoggingUtil.warn(LOGGER,
						"Job is not in the scheduler now but status of job instance in the database is still active.",
						null);
			}

		}
		return isJobRunning;
	}

}
