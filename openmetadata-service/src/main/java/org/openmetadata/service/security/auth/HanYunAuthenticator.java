package org.openmetadata.service.security.auth;

import static org.openmetadata.service.resources.teams.UserResource.USER_PROTECTED_FIELDS;

import at.favre.lib.crypto.bcrypt.BCrypt;
import freemarker.template.TemplateException;
import java.io.IOException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.openmetadata.api.configuration.hanyun.SSOConfiguration;
import org.openmetadata.schema.api.security.AuthenticationConfiguration;
import org.openmetadata.schema.auth.BasicAuthMechanism;
import org.openmetadata.schema.auth.LoginRequest;
import org.openmetadata.schema.auth.RefreshToken;
import org.openmetadata.schema.entity.teams.AuthenticationMechanism;
import org.openmetadata.schema.entity.teams.User;
import org.openmetadata.service.Entity;
import org.openmetadata.service.OpenMetadataApplicationConfig;
import org.openmetadata.service.auth.JwtResponse;
import org.openmetadata.service.jdbi3.TokenRepository;
import org.openmetadata.service.jdbi3.UserRepository;
import org.openmetadata.service.security.SecurityUtil;
import org.openmetadata.service.util.*;

@Slf4j
public class HanYunAuthenticator implements AuthenticatorHandler {
  private static final String PLATFORM_OAUTH_TOKEN_PATH = "/hanyun/sys/sso/oauth/token";
  private static final String PLATFORM_OAUTH_USERINFO_PATH = "/hanyun/sys/sso/oauth/user-info";
  private static final String PLATFORM_ACCESS_TOKEN = "access_token";
  private static final String PLATFORM_USER = "loginName";

  private UserRepository userRepository;
  private TokenRepository tokenRepository;
  private OpenMetadataApplicationConfig config;
  private SSOConfiguration hanYunSSOConfiguration;
  private AuthenticationConfiguration authenticationConfiguration;
  private String platform_url;

  private static final HanYunAuthenticator INSTANCE = new HanYunAuthenticator();

  public static HanYunAuthenticator getInstance() {
    return INSTANCE;
  }

  @Override
  public void init(OpenMetadataApplicationConfig config) {
    this.userRepository = (UserRepository) Entity.getEntityRepository(Entity.USER);
    this.tokenRepository = Entity.getTokenRepository();
    this.config = config;
    this.hanYunSSOConfiguration =
        (null == config.getHanYunConfiguration() ? null : config.getHanYunConfiguration().getSso());
    this.authenticationConfiguration = config.getAuthenticationConfiguration();
    this.platform_url = getCAS();
  }

  @Override
  public RefreshToken createRefreshTokenForLogin(UUID currentUserId) {
    // just delete the existing token
    RefreshToken newRefreshToken = TokenUtil.getRefreshToken(currentUserId, UUID.randomUUID());
    // save Refresh Token in Database
    tokenRepository.insertToken(newRefreshToken);

    return newRefreshToken;
  }

  @Override
  public JwtResponse loginUser(LoginRequest loginRequest) throws IOException, TemplateException {
    // 低代码用户中心返回的授权码
    String code = loginRequest.getEmail();

    Map<String, String> tokenParams = new HashMap<>();
    tokenParams.put("client_id", getClientId());
    tokenParams.put("client_secret", getClientSecret());
    tokenParams.put("grant_type", getGrantType());
    tokenParams.put("code", code);
    String tokenUrl = HttpClientUtils.appendParams(platform_url + PLATFORM_OAUTH_TOKEN_PATH, tokenParams);
    String tokenResult = HttpClientUtils.doPost(tokenUrl);
    JsonObject tokenResultJson = JsonUtils.readJson(tokenResult).asJsonObject();

    if (!tokenResultJson.containsKey(PLATFORM_ACCESS_TOKEN)) {
      throw new RuntimeException(String.format("通过授权码 [%s] 的验证信息：%s", code, tokenResultJson));
    }

    Map<String, String> userInfoParams = new HashMap<>();
    userInfoParams.put("access_token", tokenResultJson.getString(PLATFORM_ACCESS_TOKEN));
    userInfoParams.put("client_id", getClientId());
    String userInfoResult = HttpClientUtils.doGet(platform_url + PLATFORM_OAUTH_USERINFO_PATH, userInfoParams);
    LOG.info("汉云平台单点登录用户信息：" + userInfoResult);
    JsonObject userInfoJson = JsonUtils.readJson(userInfoResult).asJsonObject();

    String userName = userInfoJson.getString(PLATFORM_USER);

    // 自登录
    User user;
    boolean exist = userRepository.checkNameAlreadyExists(userName);
    if (!exist) {
      // 2024年5月6日 汉云单点登陆，用户不存在的情况下自动同步该用户到元数据系统中
      String email = userName + "@" + SecurityUtil.getDomain(config);

      User newUser =
          new User()
              .withId(UUID.randomUUID())
              .withName(userName)
              .withFullyQualifiedName(userName)
              .withEmail(email)
              .withIsBot(false)
              .withIsAdmin(true)
              .withIsEmailVerified(true)
              .withUpdatedBy(userName)
              .withUpdatedAt(System.currentTimeMillis())
          //                  .withTeams(EntityUtil.toEntityReferences(create.getTeams(), Entity.TEAM))
          //                  .withRoles(EntityUtil.toEntityReferences(create.getRoles(), Entity.ROLE))
          ;

      String genPWD = String.format("%s@lowcode") /*PasswordUtil.generateRandomPassword()*/;
      LOG.info("用户：{}，密码：{}", userName, genPWD);
      String newHashedPwd = BCrypt.withDefaults().hashToString(12, genPWD.toCharArray());
      newUser.withAuthenticationMechanism(
          new AuthenticationMechanism()
              .withAuthType(AuthenticationMechanism.AuthType.BASIC)
              .withConfig(new BasicAuthMechanism().withPassword(newHashedPwd)));
      user = userRepository.createInternal(newUser);
    } else {
      user = lookUserInProvider(userName);
    }

    // 将jwt的失效设置为最大，即可认为无失效期限
    JwtResponse response = getJwtResponse(user, ChronoField.EPOCH_DAY.range().getMaximum());

    return response;
  }

  @Override
  public void checkIfLoginBlocked(String userName) {
    // ignore
  }

  @Override
  public void recordFailedLoginAttempt(String providedIdentity, User user) throws TemplateException, IOException {
    // ignore
  }

  @Override
  public void validatePassword(String providedIdentity, User storedUser, String reqPassword)
      throws TemplateException, IOException {
    // ignore
  }

  @Override
  public User lookUserInProvider(String userName) {

    return userRepository.getByName(
        null, userName, new EntityUtil.Fields(Set.of(USER_PROTECTED_FIELDS), USER_PROTECTED_FIELDS));
  }

  private String getCAS() {
    if (null != this.hanYunSSOConfiguration && StringUtils.isNotBlank(this.hanYunSSOConfiguration.getCas())) {
      return this.hanYunSSOConfiguration.getCas();
    }
    return this.authenticationConfiguration.getAuthority();
  }

  private String getGrantType() {
    if (null != this.hanYunSSOConfiguration && StringUtils.isNotBlank(this.hanYunSSOConfiguration.getGrantType())) {
      return this.hanYunSSOConfiguration.getGrantType();
    }
    return this.authenticationConfiguration.getGrantType();
  }

  private String getClientId() {
    if (null != this.hanYunSSOConfiguration && StringUtils.isNotBlank(this.hanYunSSOConfiguration.getClientId())) {
      return this.hanYunSSOConfiguration.getClientId();
    }
    return this.authenticationConfiguration.getClientId();
  }

  private String getClientSecret() {
    if (null != this.hanYunSSOConfiguration && StringUtils.isNotBlank(this.hanYunSSOConfiguration.getClientSecret())) {
      return this.hanYunSSOConfiguration.getClientSecret();
    }
    return this.authenticationConfiguration.getClientSecret();
  }
}
