import { Component, OnInit } from '@angular/core';
import { Title } from '@angular/platform-browser';
import { LanguageSelectorComponent } from '../components/language-selector/language-selector.component';

import { BciImprintComponent, ModalWindowService, BreadcrumbsService, SidebarNavItem, BciSidebarService } from '@bci-web-core/core';
import { TenantSelectionComponent } from '../components/tenant-selection/tenant-selection.component';
import { AuthService, User } from '../auth/auth.service';
import { LangChangeEvent, TranslateService } from '@ngx-translate/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-admin',
  templateUrl: './admin.component.html',
  styleUrls: ['./admin.component.scss'],
})
export class AdminComponent implements OnInit {
  /**Local variables */
  email: any;
  title = 'RideCare';

  /**Body NavItem */
  recordingOverviewItem: SidebarNavItem = {
    title: 'Recording Overview',
    url: '/recording-overview',
    position: 0,
    icon: 'bosch-ic-video-record',
  };
  sidebarLinks: SidebarNavItem[] = [this.recordingOverviewItem];

  constructor(
    private titleService: Title,
    private modalWindowService: ModalWindowService,
    private sidebarService: BciSidebarService,
    private router: Router,
    public translateService: TranslateService,
    public breadcrumbsService: BreadcrumbsService,
    public authService: AuthService
  ) {}

  /**Aplication state */
  ngOnInit() {
    this.titleService.setTitle(this.title);
    const language = localStorage.getItem('lang') ? localStorage.getItem('lang') : this.translateService.getBrowserLang().slice(0, 2);
    this.translateService.use(language.match(/en|de/) ? language : 'en');
    this.translateService.onLangChange.subscribe((event: LangChangeEvent) => {
      localStorage.setItem('lang', event.lang);
      this.getTranslations();
      this.breadcrumbsService.setNavigationItems(this.sidebarLinks);
    });
    this.sidebarService.setSidebarState(false);
    this.authService.user$.subscribe((user: User) => {
      this.email = user?.email ?? '';
    });
  }

  private getTranslations() {
    this.translateService.get(['APP', 'SIDEBAR', 'LANGUAGE', 'RECORDING_OVERVIEW']).subscribe((res: any) => {
      this.title = res.APP.name;
      this.recordingOverviewItem.title = res.RECORDING_OVERVIEW.title;
    });
  }

  onAbout() {
    this.modalWindowService.openDialogWithComponent(BciImprintComponent);
  }

  onChangeTenant() {
    this.modalWindowService.openDialogWithComponent(TenantSelectionComponent);
  }

  onLogoutClick() {
    this.authService.logout().then(() => {
      this.router.navigate(['/']);
    });
  }

  onLanguageClick() {
    this.modalWindowService.openDialogWithComponent(LanguageSelectorComponent);
  }
}
