package org.alien4cloud.plugin.kubernetes.modifier;

import alien4cloud.paas.wf.WorkflowSimplifyService;
import alien4cloud.paas.wf.WorkflowsBuilderService;
import alien4cloud.paas.wf.util.WorkflowUtils;
import alien4cloud.paas.wf.validation.WorkflowValidator;
import alien4cloud.topology.TopologyService;
import alien4cloud.tosca.context.ToscaContextual;
import alien4cloud.utils.PropertyUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.alien4cloud.alm.deployment.configuration.flow.FlowExecutionContext;
import org.alien4cloud.plugin.kubernetes.AbstractKubernetesModifier;
import org.alien4cloud.tosca.model.definitions.*;
import org.alien4cloud.tosca.model.templates.*;
import org.alien4cloud.tosca.model.workflow.Workflow;
import org.alien4cloud.tosca.normative.constants.NormativeWorkflowNameConstants;
import org.alien4cloud.tosca.utils.TopologyNavigationUtil;
import org.springframework.stereotype.Component;

import java.util.*;

import static alien4cloud.utils.AlienUtils.safe;
import static org.alien4cloud.plugin.kubernetes.modifier.KubeTopologyUtils.*;
import static org.alien4cloud.tosca.utils.ToscaTypeUtils.isOfType;

/**
 * Remove delete steps for nodes when some namespace resources are found and resource is related to a name resource.
 */
@Slf4j
@Component(value = "kubernetes-cleanuninstall-modifier")
public class KubernetesCleanUninstallModifier extends AbstractKubernetesModifier {

    @Resource
    private WorkflowSimplifyService workflowSimplifyService;

    @Resource
    private WorkflowsBuilderService workflowBuilderService;

    @Resource
    private TopologyService topologyService;

    @Override
    @ToscaContextual
    public void process(Topology topology, FlowExecutionContext context) {
        log.info("Processing topology " + topology.getId());

        try {
            WorkflowValidator.disableValidationThreadLocal.set(true);
            doProcess(new KubernetesModifierContext(topology, context));
        } catch (Exception e) {
            context.getLog().error("KubernetesCleanUninstallModifier Can't process");
            log.warn("KubernetesCleanUninstallModifier Can't process ", e);
        } finally {
            WorkflowValidator.disableValidationThreadLocal.remove();
        }
    }


    private void removeDeleteOperations(Map<String, NodeTemplate> nodeTemplates, Topology topology) {
        Workflow uninstallWorkflow = topology.getWorkflow(NormativeWorkflowNameConstants.UNINSTALL);
        if (uninstallWorkflow != null) {
            Set<String> stepsToRemove = Sets.newHashSet();
            uninstallWorkflow.getSteps().forEach((s, workflowStep) -> {
                if (nodeTemplates.containsKey(workflowStep.getTarget())) {
                    stepsToRemove.add(s);
                }
            });
            for (String stepId : stepsToRemove) {
                WorkflowUtils.removeStep(uninstallWorkflow, stepId, true);
            }
        }
    }

    private void doProcess(KubernetesModifierContext context) {
        String providedNamespace = getProvidedMetaproperty(context.getFlowExecutionContext(), K8S_NAMESPACE_METAPROP_NAME);
        if (providedNamespace != null) {
            return;
        }

        Topology topology = context.getTopology();

        // Cache The Type Loader
        topologyService.prepareTypeLoaderCache(topology);

        Map<String, NodeTemplate> nodeTemplatesToBoost = Maps.newHashMap();
        // all the namespace resources found in the topology
        Set<String> namespaces = Sets.newHashSet();
        Set<NodeTemplate> resourcesNodes = TopologyNavigationUtil.getNodesOfType(topology, K8S_TYPES_BASE_RESOURCE, true);
        for (NodeTemplate resourceNode : resourcesNodes) {

            boolean thisIsNameSpaceResource = false;
            AbstractPropertyValue apv = PropertyUtil.getPropertyValueFromPath(resourceNode.getProperties(), "resource_type");
            if (apv != null && apv instanceof ScalarPropertyValue) {
                String resourceType = ((ScalarPropertyValue)apv).getValue();
                if (resourceType.equals("namespaces")) {
                    String namespace = PropertyUtil.getScalarPropertyValueFromPath(resourceNode.getProperties(), "namespace");
                    namespaces.add(namespace);
                    thisIsNameSpaceResource = true;
                }
            }
            if (!thisIsNameSpaceResource) {
                nodeTemplatesToBoost.put(resourceNode.getName(), resourceNode);
            }
        }

        if (namespaces.isEmpty()) {
            // No namespace resource found, do nothing
            return;
        }
        if (!nodeTemplatesToBoost.isEmpty()) {
            // remove from boost candidates resource nodes that don't target found namespaces
            nodeTemplatesToBoost.entrySet().removeIf(nodeTemplateEntry -> {
                String namespace = PropertyUtil.getScalarPropertyValueFromPath(nodeTemplateEntry.getValue().getProperties(), "namespace");
                return namespace == null || !namespaces.contains(namespace);
            });
            removeDeleteOperations(nodeTemplatesToBoost, topology);
        }

    }

}
