package com.axis41.core.jobs;


import com.adobe.granite.maintenance.MaintenanceConstants;
import com.axis41.core.service.ReportGenerator;
import com.axis41.core.util.StreamUtils;
import com.day.cq.dam.api.AssetManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.felix.scr.annotations.*;
import org.apache.felix.scr.annotations.Properties;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.apache.sling.event.jobs.consumer.JobExecutionContext;
import org.apache.sling.event.jobs.consumer.JobExecutionResult;
import org.apache.sling.event.jobs.consumer.JobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.query.Query;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.EMPTY;

@Component(metatype = true,
        label = "Analyze Links Maintenance Task",
        description = "Maintenance Task which analyzing links under specific path and create report")
@Service
@Properties({
        @Property(name = MaintenanceConstants.PROPERTY_TASK_NAME, value = "AnalyzeLinksTask", propertyPrivate = true),
        @Property(name = MaintenanceConstants.PROPERTY_TASK_TITLE, value = "Analyze Links", propertyPrivate = true),
        @Property(label = "Analyze path", value = "/content/ag-www", name = "analyze.path",
                description = "Path to analyze pages."),
        @Property(name = JobConsumer.PROPERTY_TOPICS, value = MaintenanceConstants.TASK_TOPIC_PREFIX
                + "AnalyzeLinksTask", propertyPrivate = true)})
public class AnalyzeLinksTask implements JobExecutor {
    private static final Logger log = LoggerFactory.getLogger(AnalyzeLinksTask.class);

    private static final String ANALYZE_PATH = "analyze.path";
    private String analyzePath;

    //search queries
    private static final String FIND_PAGES = "SELECT * FROM [cq:Page] AS page WHERE ISDESCENDANTNODE(page,'%s')";
    private static final String FIND_TEXT_COMPONENTS = "SELECT * FROM [nt:unstructured] AS s WHERE ISDESCENDANTNODE(s,'%s')\n"
            + "AND (s.[sling:resourceType]='ag-www/components/global/text' AND s.text IS NOT NULL)";
    private static final String HTML_A_HREF_TAG_PATTERN = "href=\"(.*?)\"";
    //excludes values from href attribute
    private static final String JAVASCRIPT_TYPE_LINK = "javascript:";
    private static final String MAILTO_TYPE_LINK = "mailto:";
    private static final String HASH_TYPE_LINK = "#";

    private static final String PATH_TO_REPORT = "/content/dam/linkReports/report_";
    private static final String EXTENSION = ".xls";

    @Reference
    private ResourceResolverFactory resolverFactory;

    @Reference
    private ReportGenerator reportGenerator;

    @Activate
    protected void activate(Map<String, Object> properties) throws Exception {
        this.analyzePath = (String) properties.get(ANALYZE_PATH);
    }

    @Override
    public JobExecutionResult process(Job job, JobExecutionContext context) {
        log.info("========== Analyze Links Task is started =======");

        final ResourceResolver resourceResolver = getResourceResolver();
        if (resourceResolver == null) {
            return context.result().message("Cannot obtain the resource resolver").failed();
        }

        final Map<String, Set<String>> result = new HashMap<>();
        StreamUtils.asStream(findPages(resourceResolver, analyzePath))
                .peek(page -> log.info("Found page - {}", page.getPath()))
                .forEach(page -> result.put(page.getPath(), findPageLinks(resourceResolver, page)));
        generateReport(result, resourceResolver);
        closeResourceResolver(resourceResolver);

        log.info("========== Analyze Links Task is completed =======");
        return context.result().message("Analyze Links Task is completed").succeeded();
    }

    private Set<String> findPageLinks(final ResourceResolver resourceResolver, final Resource pageResource) {
        final Set<String> pageLinks = new HashSet<>();
        if (resourceResolver == null || pageLinks == null) return pageLinks;

        final String path = pageResource.getPath();
        StreamUtils.asStream(findTextComponents(resourceResolver, path))
                .peek(component -> log.info("Found component - {}", component.getPath()))
                .map(component -> getPropertyFromResource(component, "text"))
                .filter(StringUtils::isNotBlank)
                .map(this::findLinks)
                .forEach(pageLinks::addAll);
        return pageLinks;
    }

    private String getPropertyFromResource(final Resource resource, final String propertyName) {
        return Optional.ofNullable(resource)
                .map(Resource::getValueMap)
                .map(valueMap -> valueMap.get(propertyName, EMPTY))
                .orElse(EMPTY);
    }

    private void generateReport(final Map<String, Set<String>> result, final ResourceResolver resourceResolver) {
            final Workbook workbook = reportGenerator.generate(result, resourceResolver);
        final AssetManager manager = resourceResolver.adaptTo(AssetManager.class);
        if (manager == null) return;

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            workbook.write(bos);
            byte[] bytes = bos.toByteArray();
            InputStream inputStream = new ByteArrayInputStream(bytes);

            manager.createAsset(getReportName(), inputStream, "application/vnd.ms-excel", true);
        } catch (IOException e) {
            log.error("Error during covert output stream into input stream", e);
        }
    }

    private String getReportName() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");
        LocalDateTime now = LocalDateTime.now();
        String currentTime = dtf.format(now);
        return PATH_TO_REPORT.concat(currentTime).concat(EXTENSION);
    }

    /**
     * This method finds all the links
     *
     * @param text is the value associated with String property in a node.
     * @return all the links which match the pattern
     */
    public List<String> findLinks(final String text) {
        List<String> links = new ArrayList<>();
        final Matcher matcher = Pattern.compile(HTML_A_HREF_TAG_PATTERN, Pattern.DOTALL).matcher(text);
        while (matcher.find()) {
            final MatchResult matchResult = matcher.toMatchResult();
            final String link = matchResult.group(1);
            if (StringUtils.isNotBlank(link) && isLinkAllowed(link)) {
                links.add(link.replace("\"", ""));
            }
        }
        return links;
    }

    private boolean isLinkAllowed(final String link) {
        return !(StringUtils.startsWith(link, JAVASCRIPT_TYPE_LINK)
                || StringUtils.startsWith(link, MAILTO_TYPE_LINK)
                || StringUtils.startsWith(link, HASH_TYPE_LINK));
    }

    private Iterator<Resource> findPages(final ResourceResolver resolver, final String path) {
        return createQuery(resolver, FIND_PAGES, path);
    }

    private Iterator<Resource> findTextComponents(final ResourceResolver resolver, final String path) {
        return createQuery(resolver, FIND_TEXT_COMPONENTS, path);
    }

    private Iterator<Resource> createQuery(final ResourceResolver resolver, final String query, final String path) {
        return resolver.findResources(String.format(query, path),
                Query.JCR_SQL2);
    }

    private ResourceResolver getResourceResolver() {
        ResourceResolver resourceResolver = null;
        try {
            resourceResolver = resolverFactory.getAdministrativeResourceResolver(null);
        } catch (LoginException e) {
            log.error("Error during obtaining the resource resolver ", e);
        }
        return resourceResolver;
    }

    private void closeResourceResolver(final ResourceResolver resourceResolver) {
        if (resourceResolver != null && resourceResolver.isLive()) {
            resourceResolver.close();
        }
    }
}