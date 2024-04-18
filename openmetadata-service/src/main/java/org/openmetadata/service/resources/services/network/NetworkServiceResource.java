package org.openmetadata.service.resources.services.network;

import static org.openmetadata.common.utils.CommonUtil.listOf;

import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.json.JsonPatch;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import org.openmetadata.schema.api.data.RestoreEntity;
import org.openmetadata.schema.api.services.CreateNetworkService;
import org.openmetadata.schema.entity.services.NetworkService;
import org.openmetadata.schema.entity.services.ServiceType;
import org.openmetadata.schema.type.*;
import org.openmetadata.service.Entity;
import org.openmetadata.service.jdbi3.NetworkServiceRepository;
import org.openmetadata.service.resources.Collection;
import org.openmetadata.service.resources.services.ServiceEntityResource;
import org.openmetadata.service.security.Authorizer;
import org.openmetadata.service.util.JsonUtils;
import org.openmetadata.service.util.ResultList;

@Path("/v1/services/networkServices")
@Tag(name = "Network Services")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Collection(name = "networkServices")
public class NetworkServiceResource
    extends ServiceEntityResource<NetworkService, NetworkServiceRepository, NetworkConnection> {
  public static final String COLLECTION_PATH = "v1/services/networkServices/";
  public static final String FIELDS = "pipelines,owner,tags,domain";

  @Override
  public NetworkService addHref(UriInfo uriInfo, NetworkService service) {
    super.addHref(uriInfo, service);
    Entity.withHref(uriInfo, service.getPipelines());
    return service;
  }

  public NetworkServiceResource(Authorizer authorizer) {
    super(Entity.NETWORK_SERVICE, authorizer, ServiceType.NETWORK);
  }

  @Override
  protected List<MetadataOperation> getEntitySpecificOperations() {
    addViewOperation("pipelines", MetadataOperation.VIEW_BASIC);
    return listOf(MetadataOperation.VIEW_USAGE, MetadataOperation.EDIT_USAGE);
  }

  public static class NetworkServiceList extends ResultList<NetworkService> {
    /* Required for serde */
  }

  @GET
  @Operation(
      operationId = "listNetworkService",
      summary = "List Network services",
      description =
          "Get a list of network services. Use cursor-based pagination to limit the number "
              + "entries in the list using `limit` and `before` or `after` query params.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "List of network services",
            content =
                @Content(
                    mediaType = "application/json",
                    schema =
                        @Schema(
                            implementation =
                                org.openmetadata.service.resources.services.network.NetworkServiceResource
                                    .NetworkServiceList.class)))
      })
  public ResultList<NetworkService> list(
      @Context UriInfo uriInfo,
      @Context SecurityContext securityContext,
      @Parameter(
              description = "Fields requested in the returned resource",
              schema = @Schema(type = "string", example = FIELDS))
          @QueryParam("fields")
          String fieldsParam,
      @Parameter(description = "Filter services by domain", schema = @Schema(type = "string", example = "Marketing"))
          @QueryParam("domain")
          String domain,
      @Parameter(description = "Limit number services returned. (1 to 1000000, " + "default 10)")
          @DefaultValue("10")
          @Min(0)
          @Max(1000000)
          @QueryParam("limit")
          int limitParam,
      @Parameter(description = "Returns list of services before this cursor", schema = @Schema(type = "string"))
          @QueryParam("before")
          String before,
      @Parameter(description = "Returns list of services after this cursor", schema = @Schema(type = "string"))
          @QueryParam("after")
          String after,
      @Parameter(
              description = "Include all, deleted, or non-deleted entities.",
              schema = @Schema(implementation = Include.class))
          @QueryParam("include")
          @DefaultValue("non-deleted")
          Include include) {
    return listInternal(uriInfo, securityContext, fieldsParam, include, domain, limitParam, before, after);
  }

  @GET
  @Path("/{id}")
  @Operation(
      operationId = "getNetworkServiceByID",
      summary = "Get an Network service by Id",
      description = "Get a Network service by `Id`.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Network service instance",
            content =
                @Content(mediaType = "application/json", schema = @Schema(implementation = NetworkService.class))),
        @ApiResponse(responseCode = "404", description = "Network service for instance {id} is not found")
      })
  public NetworkService get(
      @Context UriInfo uriInfo,
      @Context SecurityContext securityContext,
      @Parameter(description = "Id of the Network service", schema = @Schema(type = "UUID")) @PathParam("id") UUID id,
      @Parameter(
              description = "Fields requested in the returned resource",
              schema = @Schema(type = "string", example = FIELDS))
          @QueryParam("fields")
          String fieldsParam,
      @Parameter(
              description = "Include all, deleted, or non-deleted entities.",
              schema = @Schema(implementation = Include.class))
          @QueryParam("include")
          @DefaultValue("non-deleted")
          Include include) {
    NetworkService networkService = getInternal(uriInfo, securityContext, id, fieldsParam, include);
    return decryptOrNullify(securityContext, networkService);
  }

  @GET
  @Path("/name/{name}")
  @Operation(
      operationId = "getNetworkServiceByFQN",
      summary = "Get an Network service by name",
      description = "Get a network service by the service `name`.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Network service instance",
            content =
                @Content(mediaType = "application/json", schema = @Schema(implementation = NetworkService.class))),
        @ApiResponse(responseCode = "404", description = "Network service for instance {name} is not found")
      })
  public NetworkService getByName(
      @Context UriInfo uriInfo,
      @Context SecurityContext securityContext,
      @Parameter(description = "Name of the Network service", schema = @Schema(type = "string")) @PathParam("name")
          String name,
      @Parameter(
              description = "Fields requested in the returned resource",
              schema = @Schema(type = "string", example = FIELDS))
          @QueryParam("fields")
          String fieldsParam,
      @Parameter(
              description = "Include all, deleted, or non-deleted entities.",
              schema = @Schema(implementation = Include.class))
          @QueryParam("include")
          @DefaultValue("non-deleted")
          Include include) {
    NetworkService networkService = getByNameInternal(uriInfo, securityContext, name, fieldsParam, include);
    return decryptOrNullify(securityContext, networkService);
  }

  //    @PUT
  //    @Path("/{id}/testConnectionResult")
  //    @Operation(
  //            operationId = "addTestConnectionResult",
  //            summary = "Add test connection result",
  //            description = "Add test connection result to the service.",
  //            responses = {
  //                    @ApiResponse(
  //                            responseCode = "200",
  //                            description = "Successfully updated the service",
  //                            content =
  //                            @Content(mediaType = "application/json", schema = @Schema(implementation =
  // DatabaseService.class)))
  //            })
  //    public NetworkService addTestConnectionResult(
  //            @Context UriInfo uriInfo,
  //            @Context SecurityContext securityContext,
  //            @Parameter(description = "Id of the service", schema = @Schema(type = "UUID")) @PathParam("id") UUID id,
  //            @Valid TestConnectionResult testConnectionResult) {
  //        OperationContext operationContext = new OperationContext(entityType, MetadataOperation.CREATE);
  //        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
  //        NetworkService service = repository.addTestConnectionResult(id, testConnectionResult);
  //        return decryptOrNullify(securityContext, service);
  //    }

  @GET
  @Path("/{id}/versions")
  @Operation(
      operationId = "listAllNetworkServiceVersion",
      summary = "List Network service versions",
      description = "Get a list of all the versions of a network service identified by `Id`",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "List of network service versions",
            content = @Content(mediaType = "application/json", schema = @Schema(implementation = EntityHistory.class)))
      })
  public EntityHistory listVersions(
      @Context UriInfo uriInfo,
      @Context SecurityContext securityContext,
      @Parameter(description = "Id of the Network service", schema = @Schema(type = "UUID")) @PathParam("id") UUID id) {
    EntityHistory entityHistory = super.listVersionsInternal(securityContext, id);

    List<Object> versions =
        entityHistory.getVersions().stream()
            .map(
                json -> {
                  try {
                    NetworkService networkService = JsonUtils.readValue((String) json, NetworkService.class);
                    return JsonUtils.pojoToJson(decryptOrNullify(securityContext, networkService));
                  } catch (Exception e) {
                    return json;
                  }
                })
            .collect(Collectors.toList());
    entityHistory.setVersions(versions);
    return entityHistory;
  }

  @GET
  @Path("/{id}/versions/{version}")
  @Operation(
      operationId = "getSpecificNetworkService",
      summary = "Get a version of the Network service",
      description = "Get a version of the network service by given `Id`",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "network service",
            content =
                @Content(mediaType = "application/json", schema = @Schema(implementation = NetworkService.class))),
        @ApiResponse(
            responseCode = "404",
            description = "Network service for instance {id} and version " + "{version} is not found")
      })
  public NetworkService getVersion(
      @Context UriInfo uriInfo,
      @Context SecurityContext securityContext,
      @Parameter(description = "Id of the Network service", schema = @Schema(type = "UUID")) @PathParam("id") UUID id,
      @Parameter(
              description = "network service version number in the form `major`" + ".`minor`",
              schema = @Schema(type = "string", example = "0.1 or 1.1"))
          @PathParam("version")
          String version) {
    NetworkService networkService = super.getVersionInternal(securityContext, id, version);
    return decryptOrNullify(securityContext, networkService);
  }

  @POST
  @Operation(
      operationId = "createNetworkService",
      summary = "Create an Network service",
      description = "Create a new network service.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Network service instance",
            content =
                @Content(mediaType = "application/json", schema = @Schema(implementation = NetworkService.class))),
        @ApiResponse(responseCode = "400", description = "Bad request")
      })
  public Response create(
      @Context UriInfo uriInfo, @Context SecurityContext securityContext, @Valid CreateNetworkService create) {
    NetworkService service = getService(create, securityContext.getUserPrincipal().getName());
    Response response = create(uriInfo, securityContext, service);
    decryptOrNullify(securityContext, (NetworkService) response.getEntity());
    return response;
  }

  @PUT
  @Operation(
      operationId = "createOrUpdateNetworkService",
      summary = "Update Network service",
      description = "Create a new network service or update an existing network service identified by `Id`.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Network service instance",
            content =
                @Content(mediaType = "application/json", schema = @Schema(implementation = NetworkService.class))),
        @ApiResponse(responseCode = "400", description = "Bad request")
      })
  public Response createOrUpdate(
      @Context UriInfo uriInfo, @Context SecurityContext securityContext, @Valid CreateNetworkService update) {
    NetworkService service = getService(update, securityContext.getUserPrincipal().getName());
    Response response = createOrUpdate(uriInfo, securityContext, unmask(service));
    decryptOrNullify(securityContext, (NetworkService) response.getEntity());
    return response;
  }

  @PATCH
  @Path("/{id}")
  @Operation(
      operationId = "patchNetworkService",
      summary = "Update an Network service",
      description = "Update an existing NetworkService service using JsonPatch.",
      externalDocs = @ExternalDocumentation(description = "JsonPatch RFC", url = "https://tools.ietf.org/html/rfc6902"))
  @Consumes(MediaType.APPLICATION_JSON_PATCH_JSON)
  public Response patch(
      @Context UriInfo uriInfo,
      @Context SecurityContext securityContext,
      @Parameter(description = "Id of the Network service", schema = @Schema(type = "UUID")) @PathParam("id") UUID id,
      @RequestBody(
              description = "JsonPatch with array of operations",
              content =
                  @Content(
                      mediaType = MediaType.APPLICATION_JSON_PATCH_JSON,
                      examples = {
                        @ExampleObject("[" + "{op:remove, path:/a}," + "{op:add, path: /b, value: val}" + "]")
                      }))
          JsonPatch patch) {
    return patchInternal(uriInfo, securityContext, id, patch);
  }

  @DELETE
  @Path("/{id}")
  @Operation(
      operationId = "deleteNetworkService",
      summary = "Delete an Network service by Id",
      description =
          "Delete a network services. If network (and tasks) belong to the service, it can't be " + "deleted.",
      responses = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "404", description = "Network service for instance {id} " + "is not found")
      })
  public Response delete(
      @Context UriInfo uriInfo,
      @Context SecurityContext securityContext,
      @Parameter(description = "Recursively delete this entity and it's children. (Default `false`)")
          @DefaultValue("false")
          @QueryParam("recursive")
          boolean recursive,
      @Parameter(description = "Hard delete the entity. (Default = `false`)")
          @QueryParam("hardDelete")
          @DefaultValue("false")
          boolean hardDelete,
      @Parameter(description = "Id of the Network service", schema = @Schema(type = "UUID")) @PathParam("id") UUID id) {
    return delete(uriInfo, securityContext, id, recursive, hardDelete);
  }

  @DELETE
  @Path("/name/{name}")
  @Operation(
      operationId = "deleteNetworkServiceByName",
      summary = "Delete an network service by name",
      description =
          "Delete a network services by `name`. If network (and tasks) belong to the service, it can't be "
              + "deleted.",
      responses = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "404", description = "Network service for instance {name} " + "is not found")
      })
  public Response delete(
      @Context UriInfo uriInfo,
      @Context SecurityContext securityContext,
      @Parameter(description = "Hard delete the entity. (Default = `false`)")
          @QueryParam("hardDelete")
          @DefaultValue("false")
          boolean hardDelete,
      @Parameter(description = "Name of the Network service", schema = @Schema(type = "string")) @PathParam("name")
          String name) {
    return deleteByName(uriInfo, securityContext, name, false, hardDelete);
  }

  @PUT
  @Path("/restore")
  @Operation(
      operationId = "restore",
      summary = "Restore a soft deleted Network service",
      description = "Restore a soft deleted Network service.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully restored the NetworkService ",
            content = @Content(mediaType = "application/json", schema = @Schema(implementation = NetworkService.class)))
      })
  public Response restoreTable(
      @Context UriInfo uriInfo, @Context SecurityContext securityContext, @Valid RestoreEntity restore) {
    return restoreEntity(uriInfo, securityContext, restore.getId());
  }

  private NetworkService getService(CreateNetworkService create, String user) {
    return repository
        .copy(new NetworkService(), create, user)
        .withServiceType(create.getServiceType())
        .withConnection(create.getConnection());
  }

  @Override
  protected NetworkService nullifyConnection(NetworkService service) {
    return service.withConnection(null);
  }

  @Override
  protected String extractServiceType(NetworkService service) {
    return service.getServiceType().value();
  }
}
